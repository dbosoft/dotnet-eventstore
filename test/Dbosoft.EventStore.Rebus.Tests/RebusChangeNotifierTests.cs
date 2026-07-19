using Rebus.Activation;
using Rebus.Config;
using Rebus.Transport.InMem;
using Xunit;

namespace Dbosoft.EventStore.Rebus.Tests;

public sealed class RebusChangeNotifierTests
{
    // Two separate bus instances over one shared in-memory network — a genuine cross-"process" round-trip,
    // not a mock: the writer publishes, the reader's handler receives and releases its local waiter. The
    // in-memory transport is a centralized subscription store, so a subscribe on one bus is visible to the
    // other through the shared network.
    [Fact]
    public async Task A_write_in_one_process_wakes_a_waiter_in_another()
    {
        var network = new InMemNetwork();

        // The handler must be registered before the bus starts, but the notifier needs the started bus — so
        // the handler reads the notifier through a holder that is filled in once the bus is up.
        var reader = new NotifierHolder();
        using var readerActivator = new BuiltinHandlerActivator();
        readerActivator.Register(() => new EventStoreChangedHandler(reader.Notifier));
        Configure.With(readerActivator)
            .Transport(t => t.UseInMemoryTransport(network, "reader"))
            .Start();
        reader.Notifier = new RebusChangeNotifier(readerActivator.Bus);
        await readerActivator.Bus.Subscribe<EventStoreChanged>();

        // Writer "process": no handler; it only publishes.
        using var writerActivator = new BuiltinHandlerActivator();
        Configure.With(writerActivator)
            .Transport(t => t.UseInMemoryTransport(network, "writer"))
            .Start();
        var writerNotifier = new RebusChangeNotifier(writerActivator.Bus);

        var wait = reader.Notifier.WaitForChangeAsync("banking");

        await writerNotifier.NotifyAsync("banking");

        await wait.WaitAsync(TimeSpan.FromSeconds(5)); // the remote write's ping woke the reader
    }

    [Fact]
    public async Task Notify_wakes_a_waiter_in_the_same_process_without_the_round_trip()
    {
        using var activator = new BuiltinHandlerActivator();
        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "solo"))
            .Start();
        var notifier = new RebusChangeNotifier(activator.Bus);

        var wait = notifier.WaitForChangeAsync("d");
        await notifier.NotifyAsync("d");

        await wait.WaitAsync(TimeSpan.FromSeconds(2)); // released locally, no message needed
    }

    // Lets the pre-Start handler registration reference a notifier that only exists once the bus is started.
    private sealed class NotifierHolder
    {
        public RebusChangeNotifier Notifier { get; set; } = null!;
    }
}
