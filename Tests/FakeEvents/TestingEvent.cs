using YoungMessaging.Abstractions;

namespace YoungMessaging.Tests.FakeEvents{
    public class TestingEvent : Event{
        public string TestString{get;set;}
        public int TestInt{get;set;}
    }
}