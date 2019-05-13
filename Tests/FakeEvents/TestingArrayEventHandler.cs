using System;
using System.Threading;
using System.Threading.Tasks;
using YoungMessaging.Abstractions;
using YoungMessaging.Tests.FakeEvents;
namespace YoungMessaging.Tests.FakeEvents{
    public class TestingArrayEventHandler : IArrayEventHandler<TestingEvent>
    { 
        private readonly object countLock = new object();
        public bool success = false;
        protected int count = 0;
        public int Count {get{return count;}}
        public TestingArrayEventHandler(){}
        public Task<EventResult> Handle(TestingEvent[] @event, CancellationToken token)
        {
            success = true;
            lock (countLock){
                count++;
            }
            return Task.FromResult(EventResult.Success);
        }
    }
}