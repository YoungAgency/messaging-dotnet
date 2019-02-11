using System.Threading;
using System.Threading.Tasks;
using YoungMessaging.Abstractions;
using YoungMessaging.Tests.FakeEvents;
namespace YoungMessaging.Tests.FakeEvents{
    public class TestingEventHandler : IEventHandler<TestingEvent>
    { 
        private readonly object countLock = new object();
        public bool success = false;
        protected int count = 0;
        public int Count {get{return count;}}
        public TestingEventHandler(){}
        public Task<EventResult> Handle(TestingEvent @event, CancellationToken token)
        {
            if(@event.TestString == "TestString" && @event.TestInt == 5){
                success = true;
            }
            lock (countLock){
                count++;
            }
            return Task.FromResult(EventResult.Success);
        }
    }
}