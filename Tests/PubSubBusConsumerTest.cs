using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
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

namespace Wallets.Tests {
    public class PubSubBusConsumerTest
    {
        private PubSubBusConsumer Init(){
            BusSettings busSettings = new BusSettings{BusHost = "localhost", BusPort = 8519, ProjectId = "youngplatform", SubscriptionName = "WalletsApiTest"};
            PubSubBusConsumer bus = new PubSubBusConsumer(busSettings);
            return bus;
        }

        [Fact]
        public void TestSubscribe()
        {
            //Given
           var bus = Init();
           TestingEventHandler handler = new TestingEventHandler();
           bus.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");

           PublishMessage();
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(!handler.success && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.True(handler.success);
           Assert.Equal(1,handler.count);
        }

        [Fact]
        public void TestMultipleMessages(){
            //Given
           var bus = Init();
           TestingEventHandler handler = new TestingEventHandler();
           int messagesCount = new Random().Next(1000,10000);
           PublishMultipleMessages(messagesCount);
           bus.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(handler.count < messagesCount && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.Equal(messagesCount, handler.count);
        }

        [Fact]
        public void TestPublish(){
             //Given
           var bus = Init();
           TestingEventHandler handler = new TestingEventHandler();
           bus.Subscribe<TestingEvent,TestingEventHandler>(()=>handler,"testingevent");
           bus.PublishAsync(new TestingEvent{TestInt=5, TestString="TestString"},"testingevent").GetAwaiter().GetResult();
           
           int elapsed = 0;
           TimeSpan timeout = TimeSpan.FromSeconds(15);
           while(!handler.success && (elapsed < timeout.TotalMilliseconds)){
               Thread.Sleep(100);
               elapsed += 100;
           }
           Assert.True(handler.success);
           Assert.Equal(1,handler.count);
        }

        private void PublishMessage(){
            /* // First create a topic.
            Channel channel = new Channel("localhost:8519",ChannelCredentials.Insecure);
            PublisherServiceApiClient publisherService = PublisherServiceApiClient.Create(channel);
            TopicName topicName = new TopicName("youngplatform", "testingevent");
            try{
                publisherService.CreateTopic(topicName);
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists){
            }
            
            TestingEvent fakeEvent = new TestingEvent{TestInt=5, TestString="TestString"};
            RepeatedField<PubsubMessage> messages = new RepeatedField<PubsubMessage>();
            messages.Add(new PubsubMessage{Data= ByteString.CopyFrom(JsonConvert.SerializeObject(fakeEvent), Encoding.UTF8)});
            var result = publisherService.Publish(topicName,messages);*/
            TestingEvent fakeEvent = new TestingEvent{TestInt=5, TestString="TestString"};
            var bus = Init();
            var result = bus.PublishAsync(fakeEvent,"testingevent").GetAwaiter().GetResult();
            Console.Write(result.ToString());
        }

        private void PublishMultipleMessages(int number){
             // First create a topic.
            Channel channel = new Channel("localhost:8519",ChannelCredentials.Insecure);
            PublisherServiceApiClient publisherService = PublisherServiceApiClient.Create(channel);
            TopicName topicName = new TopicName("youngplatform", "testingevent");
            try{
                publisherService.CreateTopic(topicName);
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists){
            }
            // Publish a message to the topic using PublisherClient.
            
            TestingEvent fakeEvent = new TestingEvent{TestInt=5, TestString="TestString"};
            for(int i = 0; i < number; i++){
                Thread.Sleep(5);
                RepeatedField<PubsubMessage> messages = new RepeatedField<PubsubMessage>();
                messages.Add(new PubsubMessage{Data= ByteString.CopyFrom(JsonConvert.SerializeObject(fakeEvent), Encoding.UTF8)});
                var result = publisherService.Publish(topicName,messages);
            }
        }


    }
}