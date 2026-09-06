using System.Collections.Concurrent;
using JasperFx.Blocks;
using Shouldly;

namespace CoreTests.Blocks;

public class BlockBackpressureTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);
    private static TaskCompletionSource signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task synchronous_Post_preserves_every_ID_after_default_capacity_is_exceeded()
    {
        const int total = 25000;
        var entered = signal();
        var release = signal();
        var full = signal();
        var processed = new ConcurrentBag<int>();
        await using var block = new Block<int>(async (id, _) =>
        {
            if (id == 0)
            {
                entered.SetResult();
                await release.Task.WaitAsync(Timeout);
            }
            processed.Add(id);
        });
        block.Post(0);
        await entered.Task.WaitAsync(Timeout);
        var producer = Task.Run(() =>
        {
            for (var id = 1; id < total; id++)
            {
                if (id == 10001) full.SetResult();
                block.Post(id);
            }
        });
        bool blocked;
        try
        {
            await full.Task.WaitAsync(Timeout);
            // The consumer cannot drain before this point: at least 10,000 writes were offered.
            // A bounded observation distinguishes a blocked writer from the old completed/lossy writer.
            await Task.WhenAny(producer, Task.Delay(100));
            blocked = !producer.IsCompleted;
        }
        finally
        {
            release.TrySetResult();
        }
        await producer.WaitAsync(Timeout);
        await block.WaitForCompletionAsync().WaitAsync(Timeout);
        processed.Order().ShouldBe(Enumerable.Range(0, total));
        blocked.ShouldBeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task configured_capacity_backpressures_until_the_consumer_is_released(bool asynchronous)
    {
        var entered = signal();
        var release = signal();
        var attempting = signal();
        var processed = new List<int>();
        await using var block = new Block<int>(1, 1, async (id, _) =>
        {
            if (id == 0)
            {
                entered.SetResult();
                await release.Task.WaitAsync(Timeout);
            }
            processed.Add(id);
        });
        block.Post(0);
        await entered.Task.WaitAsync(Timeout);
        block.Post(1);
        var producer = Task.Run(async () =>
        {
            attempting.SetResult();
            if (asynchronous) await block.PostAsync(2);
            else block.Post(2);
        });
        bool blocked;
        try
        {
            await attempting.Task.WaitAsync(Timeout);
            await Task.WhenAny(producer, Task.Delay(100));
            blocked = !producer.IsCompleted;
        }
        finally
        {
            release.TrySetResult();
        }
        await producer.WaitAsync(Timeout);
        await block.WaitForCompletionAsync().WaitAsync(Timeout);
        blocked.ShouldBeTrue();
        processed.ShouldBe(new[] { 0, 1, 2 });
    }

    [Fact]
    public async Task unbounded_Post_finishes_before_a_held_consumer_is_released()
    {
        var entered = signal();
        var release = signal();
        var processed = new List<int>();
        await using var block = new Block<int>(1, Block<int>.Unbounded, async (id, _) =>
        {
            if (id == 0)
            {
                entered.SetResult();
                await release.Task.WaitAsync(Timeout);
            }
            processed.Add(id);
        });
        block.Post(0);
        await entered.Task.WaitAsync(Timeout);
        var producer = Task.Run(() =>
        {
            for (var id = 1; id < 25000; id++) block.Post(id);
        });
        try
        {
            await producer.WaitAsync(Timeout);
        }
        finally
        {
            release.TrySetResult();
        }
        await producer.WaitAsync(Timeout);
        await block.WaitForCompletionAsync().WaitAsync(Timeout);
        processed.ShouldBe(Enumerable.Range(0, 25000));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Complete_releases_a_blocked_Post_and_reports_the_rejected_item(bool errorHandlerThrows)
    {
        var entered = signal();
        var release = signal();
        var attempting = signal();
        var errors = new ConcurrentBag<(int, Exception)>();
        var processed = new List<int>();
        await using var block = new Block<int>(1, 1, async (id, _) =>
        {
            if (id == 0)
            {
                entered.SetResult();
                await release.Task.WaitAsync(Timeout);
            }
            processed.Add(id);
        });
        block.OnError = (id, error) =>
        {
            errors.Add((id, error));
            if (errorHandlerThrows) throw new InvalidOperationException("Error handler failure");
        };
        block.Post(0);
        await entered.Task.WaitAsync(Timeout);
        block.Post(1);
        var producer = Task.Run(() =>
        {
            attempting.SetResult();
            block.Post(2);
        });
        try
        {
            await attempting.Task.WaitAsync(Timeout);
            // No reader can finish here, so the in-flight count only serves as a writer-entry
            // handshake, never as evidence of delivery. Avoid a post-after-complete race.
            SpinWait.SpinUntil(() => block.Count == 3, Timeout).ShouldBeTrue();
            block.Complete();
            await producer.WaitAsync(Timeout);
            block.Post(3); // Post after completion is ignored, as is PostAsync.
            await block.PostAsync(4);
        }
        finally
        {
            block.Complete();
            release.TrySetResult();
        }
        await producer.WaitAsync(Timeout);
        await block.WaitForCompletionAsync().WaitAsync(Timeout);
        processed.ShouldBe(new[] { 0, 1 });
        errors.Count.ShouldBe(1);
        errors.Single().Item1.ShouldBe(2);
        errors.Single().Item2.ShouldBeOfType<System.Threading.Channels.ChannelClosedException>();
    }

    [Fact]
    public async Task processing_error_is_reported_and_remaining_items_are_processed()
    {
        var error = signal();
        var processed = new ConcurrentBag<int>();
        await using var block = new Block<int>(1, 1, (id, _) =>
        {
            if (id == 0) throw new InvalidOperationException("Expected processing failure");
            processed.Add(id);
            return Task.CompletedTask;
        });
        block.OnError = (id, exception) =>
        {
            id.ShouldBe(0);
            exception.ShouldBeOfType<InvalidOperationException>();
            error.SetResult();
        };
        var producer = Task.Run(() =>
        {
            for (var id = 0; id < 100; id++) block.Post(id);
        });
        await producer.WaitAsync(Timeout);
        await error.Task.WaitAsync(Timeout);
        await block.WaitForCompletionAsync().WaitAsync(Timeout);
        processed.Order().ShouldBe(Enumerable.Range(1, 99));
    }

    [Theory]
    [InlineData(-2)]
    public void invalid_capacity_is_rejected(int capacity)
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
            new Block<int>(1, capacity, (_, _) => Task.CompletedTask));
    }
}
