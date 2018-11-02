using System.Threading;
using System.Threading.Tasks;
using YoungMessaging.Abstractions;
using YoungMessaging.Tests.FakeEvents;
namespace YoungMessaging.Tests.FakeEvents{
    public class TestingEventHandler : IEventHandler<TestingEvent>
    {
        public bool success = false;
        public int count = 0;
        public TestingEventHandler(){}
        public async Task<EventResult> Handle(TestingEvent @event, CancellationToken token)
        {
            if(@event.TestString == "TestString" && @event.TestInt == 5){
                success = true;
            }
            count++;
            return EventResult.Success;
        }
    }
}