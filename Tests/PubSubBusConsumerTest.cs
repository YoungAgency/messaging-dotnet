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
using Google.Api.Gax.Grpc;
using Xunit.Abstractions;
using System.Diagnostics;

namespace Wallets.Tests;
public class PubSubBusConsumerTest
{
    ITestOutputHelper _output;
    public PubSubBusConsumerTest(ITestOutputHelper output)
    {
        _output = output;
    }
    private PubSubBusConsumer Init()
    {
        BusSettings busSettings = new BusSettings { BusHost = "localhost", BusPort = 8519, ProjectId = "youngplatform", SubscriptionName = "WalletsApiTest", Token = "token" };
        PubSubBusConsumer bus = new PubSubBusConsumer(busSettings);
        return bus;
    }
    /*private void CreateTestTopic(BusSettings _busSettings){
        PublisherServiceApiClient publisherService;

        if(_busSettings.BusHost != null && _busSettings.BusHost != ""){
            Channel channel = new Channel(_busSettings.BusHost+":"+_busSettings.BusPort,ChannelCredentials.Insecure);
            publisherService = PublisherServiceApiClient.Create(channel);
        }
        else {
            publisherService = PublisherServiceApiClient.Create();
        }

        TopicName topicName = new TopicName(_busSettings.ProjectId, "testingevent");

        try{
            publisherService.CreateTopic(topicName,new Google.Api.Gax.Grpc.CallSettings(null,null,CallTiming.FromTimeout(new TimeSpan(0,0,5)),null,null,null));
        }
        catch(RpcException ex)
        when(ex.StatusCode == StatusCode.AlreadyExists || ex.StatusCode == StatusCode.Unavailable){
        }
        catch(Exception ex){
            throw new Exception(ex.Message);
        }    
    }*/

    [Fact]
    public void TestSubscribe()
    {
        //Given
        var bus = Init();
        TestingEventHandler handler = new TestingEventHandler();
        bus.Subscribe<TestingEvent, TestingEventHandler>(handler, "testingevent");

        PublishMessage();
        int elapsed = 0;
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        while (!handler.success && (elapsed < timeout.TotalMilliseconds))
        {
            Thread.Sleep(100);
            elapsed += 100;
        }
        Assert.True(handler.success);
        Assert.Equal(1, handler.Count);
    }

    [Fact]
    public void TestPublish()
    {
        //Given
        var bus = Init();
        TestingEventHandler handler = new TestingEventHandler();
        bus.Subscribe<TestingEvent, TestingEventHandler>(handler, "testingevent");
        bus.PublishAsync(new TestingEvent { TestInt = 5, TestString = "TestString" }, "testingevent").GetAwaiter().GetResult();

        int elapsed = 0;
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        while (!handler.success && (elapsed < timeout.TotalMilliseconds))
        {
            Thread.Sleep(100);
            elapsed += 100;
        }
        Assert.True(handler.success);
        Assert.Equal(1, handler.Count);
    }

    [Fact]
    public void TestNoToken()
    {
        //Given
        BusSettings busSettings = new BusSettings { BusHost = "localhost", BusPort = 8519, ProjectId = "youngplatform", SubscriptionName = "WalletsApiTest", Token = "" };
        PubSubBusConsumer bus = new PubSubBusConsumer(busSettings);
        TestingEventHandler handler = new TestingEventHandler();
        bus.Subscribe<TestingEvent, TestingEventHandler>(handler, "testingevent");
        bus.PublishAsync(new TestingEvent { TestInt = 5, TestString = "TestString" }, "testingevent").GetAwaiter().GetResult();

        int elapsed = 0;
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        while (!handler.success && (elapsed < timeout.TotalMilliseconds))
        {
            Thread.Sleep(100);
            elapsed += 100;
        }
        Assert.True(handler.success);
        Assert.Equal(1, handler.Count);
    }

    [Fact]
    public void TestSubscribeArray()
    {
        //Given
        BusSettings busSettings = new BusSettings { BusHost = "localhost", BusPort = 8519, ProjectId = "youngplatform", SubscriptionName = "WalletsApiTest", Token = "" };
        PubSubBusConsumer bus = new PubSubBusConsumer(busSettings);
        TestingArrayEventHandler handler = new TestingArrayEventHandler();
        bus.SubscribeArray<TestingEvent, TestingArrayEventHandler>(handler, "testingevent");
        bus.PublishAsync(new TestingEvent[] { new TestingEvent { TestInt = 5, TestString = "TestString" }, new TestingEvent { TestInt = 5, TestString = "TestString" } }, "testingevent").GetAwaiter().GetResult();

        int elapsed = 0;
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        while (!handler.success && (elapsed < timeout.TotalMilliseconds))
        {
            Thread.Sleep(100);
            elapsed += 100;
        }
        Assert.True(handler.success);
        Assert.Equal(1, handler.Count);
    }

    private void PublishMessage()
    {
        TestingEvent fakeEvent = new TestingEvent { TestInt = 5, TestString = "TestString" };
        var bus = Init();
        var result = bus.PublishAsync(fakeEvent, "testingevent").GetAwaiter().GetResult();
    }

    private async Task PublishMultipleMessages(int number)
    {
        var bus = Init();
        TestingEvent fakeEvent = new TestingEvent { TestInt = 5, TestString = "TestString" };
        for (int i = 0; i < number; i++)
        {
            var result = await bus.PublishAsync(fakeEvent, "testingevent");
            if (!result)
            {
                throw new Exception("Error publishing");
            }
        }
    }

    [Fact]
    public async Task TestMultipleMessages()
    {
        //Given
        var bus = Init();
        TestingEventHandler handler = new TestingEventHandler();
        int messagesCount = new Random().Next(200, 600);
        bus.Subscribe<TestingEvent, TestingEventHandler>(handler, "testingevent");
        await PublishMultipleMessages(messagesCount);
        int elapsed = 0;
        TimeSpan timeout = TimeSpan.FromSeconds(30);
        while (handler.Count < messagesCount && (elapsed < timeout.TotalMilliseconds))
        {
            Thread.Sleep(100);
            elapsed += 100;
        }
        Assert.Equal(messagesCount, handler.Count);
    }

}