using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using JasperFx.Blocks;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;

namespace CoreTests.Blocks;

public class RetryBlockBackpressureTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task every_legacy_constructor_can_retry_onto_a_saturated_queue(int constructor)
    {
        const int total = 25000;
        var timeout = TimeSpan.FromSeconds(15);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processed = new ConcurrentBag<int>();
        var attempts = new ConcurrentDictionary<int, int>();
        var configured = false;
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        Func<int, CancellationToken, Task> handler = async (id, _) =>
        {
            var attempt = attempts.AddOrUpdate(id, 1, (_, n) => n + 1);
            if (id == 0 && attempt == 1)
            {
                entered.SetResult();
                await release.Task.WaitAsync(timeout);
                throw new InvalidOperationException("Retry while more than 10,000 items are queued");
            }
            processed.Add(id);
            if (processed.Count == total) finished.TrySetResult();
        };
        Action<ExecutionDataflowBlockOptions> configure = options =>
        {
            configured = true;
            options.CancellationToken.ShouldBe(cancellation.Token);
            options.SingleProducerConstrained.ShouldBeTrue();
        };
        var options = new ExecutionDataflowBlockOptions();
        using var block = constructor switch
        {
            0 => new RetryBlock<int>(handler, NullLogger.Instance, cancellation.Token, options),
            1 => new RetryBlock<int>(handler, NullLogger.Instance, cancellation.Token, configure),
            _ => new RetryBlock<int>(new LambdaItemHandler<int>(handler), NullLogger.Instance,
                cancellation.Token, configure)
        };
        block.Pauses = [TimeSpan.Zero];
        if (constructor == 0)
        {
            options.CancellationToken.ShouldBe(cancellation.Token);
            options.SingleProducerConstrained.ShouldBeTrue();
        }
        else configured.ShouldBeTrue();
        block.Post(0);
        await entered.Task.WaitAsync(timeout);
        var producer = Task.Run(() =>
        {
            for (var id = 1; id < total; id++) block.Post(id);
        });
        try
        {
            // The queue must accept all backlog without requiring its held reader to make room.
            await producer.WaitAsync(timeout);
            release.SetResult();
            // Do not Drain/Complete before the failing item has re-posted itself.
            await finished.Task.WaitAsync(timeout);
        }
        finally
        {
            release.TrySetResult();
            block.Dispose(); // Also releases a blocked writer if this regression ever returns.
            await producer.WaitAsync(timeout);
        }
        await block.DrainAsync().WaitAsync(timeout);
        processed.Order().ShouldBe(Enumerable.Range(0, total));
        attempts[0].ShouldBe(2);
        attempts.Where(x => x.Key != 0).All(x => x.Value == 1).ShouldBeTrue();
    }

    [Fact]
    public async Task cancelled_retry_block_does_not_invoke_the_handler()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var calls = 0;
        using var block = new RetryBlock<int>((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.CompletedTask;
        }, NullLogger.Instance, cancellation.Token, new ExecutionDataflowBlockOptions());
        block.Post(1);
        await block.PostAsync(2).WaitAsync(TimeSpan.FromSeconds(15));
        await block.DrainAsync().WaitAsync(TimeSpan.FromSeconds(15));
        calls.ShouldBe(0);
    }
}
