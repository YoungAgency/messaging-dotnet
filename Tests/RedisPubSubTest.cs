using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Moq;
using Newtonsoft.Json;
using YoungMessaging.Tests.FakeEvents;
using YoungMessaging.EventBus;
using YoungMessaging.Settings;
using YoungMessaging.Abstractions;
using Xunit;
using Google.Api.Gax.Grpc;
using Xunit.Abstractions;
using System.Diagnostics;

namespace Wallets.Tests {
    public class RedisPubsSubTest
    {
        ITestOutputHelper _output;
        public RedisPubsSubTest(ITestOutputHelper output){
            _output = output;
        }
        private IBusConsumer InitConsumer(){
            BusSettings busSettings = new BusSettings{BusHost = "localhost", BusPort = 6379};
            RedisPubSubConsumer bus = new RedisPubSubConsumer(busSettings);
            return bus;
        }
        private IBusProducer InitProducer(){
            BusSettings busSettings = new BusSettings{BusHost = "localhost", BusPort = 6379};
            RedisPubSubProducer bus = new RedisPubSubProducer(busSettings);
            return bus;
        }
       
        [Fact]
        public void TestSubscribe()
        {
            //Given
           var consumer = InitConsumer();
           TestingEventHandler handler = new TestingEventHandler();
           consumer.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");

           PublishMessage();
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(!handler.success && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.True(handler.success);
           Assert.Equal(1,handler.Count);
        }

        [Fact]
        public async Task TestMultipleMessages(){
            //Given
           var consumer = InitConsumer();
           TestingEventHandler handler = new TestingEventHandler();
           int messagesCount = new Random().Next(1000,2000);
           consumer.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");
           await PublishMultipleMessages(messagesCount);
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(30);
           while(handler.Count < messagesCount && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.Equal(messagesCount, handler.Count);
        }

        [Fact]
        public void TestPublish(){
             //Given
           var consumer = InitConsumer();
           var producer = InitProducer();
           TestingEventHandler handler = new TestingEventHandler();
           consumer.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");
           producer.PublishAsync(new TestingEvent{TestInt=5, TestString="TestString"},"testingevent").GetAwaiter().GetResult();
           
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(!handler.success && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.True(handler.success);
           Assert.Equal(1,handler.Count);
        }

        [Fact]
        public void TestSubscribeArray(){
           //Given
           var consumer = InitConsumer();
           var producer = InitProducer();
           TestingArrayEventHandler handler = new TestingArrayEventHandler();
           consumer.SubscribeArray<TestingEvent,TestingArrayEventHandler>(()=>handler,"testingevent");
           producer.PublishAsync(new TestingEvent[]{new TestingEvent{TestInt=5, TestString="TestString"},new TestingEvent{TestInt=5, TestString="TestString"}},"testingevent").GetAwaiter().GetResult();
           
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(!handler.success && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.True(handler.success);
           Assert.Equal(1,handler.Count);
        }

        private void PublishMessage(){
            TestingEvent fakeEvent = new TestingEvent{TestInt=5, TestString="TestString"};
            var bus = InitProducer();
            var result = bus.PublishAsync(fakeEvent,"testingevent").GetAwaiter().GetResult();
        }

        private async Task PublishMultipleMessages(int number){
            var bus = InitProducer();
            TestingEvent fakeEvent = new TestingEvent{TestInt=5, TestString="TestString"};
            for(int i = 0; i < number; i++){
                var result = await bus.PublishAsync(fakeEvent,"testingevent");
                if(!result){
                    throw new Exception("Error publishing");
                }
            }
        }
    }
}