using System;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Newtonsoft.Json;
using System.Threading;
using YoungMessaging.Abstractions;
using YoungMessaging.Settings;
using Google.Protobuf;

namespace YoungMessaging.EventBus
{
    public class PubSubBusConsumer : IBusConsumer, IBusProducer
    {
        private readonly BusSettings _busSettings;
        public PubSubBusConsumer(BusSettings busSettings)
        {
            _busSettings = busSettings;
        }

        public async Task Subscribe<T, TH>(IEventHandler<T> handler, string topicName, int maxConcurrent = 10)
            where T : Event
            where TH : IEventHandler<T>
        {

            try
            {
                string subscription = _busSettings.SubscriptionName.ToLower() + "-" + topicName.ToLower();
                CreateSubscription(topicName, subscription);

                SubscriberClient subscriber = await new SubscriberClientBuilder
                {
                    SubscriptionName = new SubscriptionName(_busSettings.ProjectId, subscription),
                    Settings = new SubscriberClient.Settings
                    {
                        FlowControlSettings = new Google.Api.Gax.FlowControlSettings(maxConcurrent, 10240)
                    }
                }.BuildAsync();

                await subscriber.StartAsync(async (PubsubMessage message, CancellationToken token) =>
                {
                    T eventMessage;
                    if (_busSettings.Token != null && _busSettings.Token != "" && (!message.Attributes.ContainsKey("token") || message.Attributes["token"] != _busSettings.Token))
                    {
                        return SubscriberClient.Reply.Ack;
                    }
                    try
                    {
                        eventMessage = JsonConvert.DeserializeObject<T>(message.Data.ToStringUtf8());
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine(ex.Message);
                        return SubscriberClient.Reply.Ack;
                    }
                    try
                    {
                        eventMessage.EventId = message.MessageId;
                        eventMessage.Timestamp = message.PublishTime.Seconds * 1000;
                    }
                    catch (NullReferenceException ex)
                    {
                        Console.WriteLine(ex.Message);
                        return SubscriberClient.Reply.Ack;
                    }
                    EventResult result = await handler.Handle(eventMessage, new CancellationToken());
                    return result == EventResult.Success ? SubscriberClient.Reply.Ack : SubscriberClient.Reply.Nack;
                });
            }
            // Restart when connection fail
            catch (RpcException ex)
            {
                Console.WriteLine(ex.Message);
                await Subscribe<T, TH>(handler, topicName, maxConcurrent);
                return;
            }
        }

        public async Task SubscribeArray<T, TH>(IArrayEventHandler<T> handler, string topicName, int maxConcurrent = 10)
          where T : Event
          where TH : IArrayEventHandler<T>
        {
            try
            {
                string subscription = _busSettings.SubscriptionName.ToLower() + "-" + topicName.ToLower();
                CreateSubscription(topicName, subscription);
                // Pull messages from the subscription using SimpleSubscriber.
                SubscriberClient subscriber = await new SubscriberClientBuilder
                {
                    SubscriptionName = new SubscriptionName(_busSettings.ProjectId, subscription),
                    Settings = new SubscriberClient.Settings
                    {
                        FlowControlSettings = new Google.Api.Gax.FlowControlSettings(maxConcurrent, null)
                    }
                }.BuildAsync();

                await subscriber.StartAsync(async (PubsubMessage message, CancellationToken token) =>
                {
                    T[] events;
                    if (_busSettings.Token != null && _busSettings.Token != "" && (!message.Attributes.ContainsKey("token") || message.Attributes["token"] != _busSettings.Token))
                    {
                        return SubscriberClient.Reply.Ack;
                    }
                    try
                    {
                        events = JsonConvert.DeserializeObject<T[]>(message.Data.ToStringUtf8());
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine(ex.Message);
                        return SubscriberClient.Reply.Ack;
                    }
                    for (int i = 0; i < events.Length; i++)
                    {
                        events[i].EventId = message.MessageId;
                        events[i].Timestamp = message.PublishTime.Seconds * 1000;
                    }

                    EventResult result = await handler.Handle(events, new CancellationToken());
                    return result == EventResult.Success ? SubscriberClient.Reply.Ack : SubscriberClient.Reply.Nack;
                });
            }
            // Restart when connection fail
            catch (RpcException ex)
            {
                Console.WriteLine(ex.Message);
                await SubscribeArray<T, TH>(handler, topicName, maxConcurrent);
                return;
            }
        }

        private void CreateSubscription(string topic, string subscription)
        {
            SubscriberServiceApiClient subscriberService;
            subscriberService = SubscriberServiceApiClient.Create();

            SubscriptionName subscriptionName = new(_busSettings.ProjectId, subscription);
            TopicName topicName = new(_busSettings.ProjectId, topic);

            try
            {
                var actualSub = subscriberService.GetSubscriptionAsync(subscriptionName);
                if (actualSub != null)
                {
                    return;
                }

                subscriberService.CreateSubscription(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 20);
            }
            catch (RpcException ex)
            when (ex.StatusCode is StatusCode.AlreadyExists or StatusCode.Unavailable)
            {
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        /*private void CreateTopic(string topic){
            PublisherServiceApiClient publisherService;
            publisherService = PublisherServiceApiClient.Create();
            
            TopicName topicName = new TopicName(_busSettings.ProjectId, topic);

            try{
                publisherService.CreateTopic(topicName,new CallSettings(null,null, new Google.Api.Gax.Expiration(),null,null,null,null;
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists || ex.StatusCode == StatusCode.Unavailable){
            }
            catch(Exception ex){
                throw new Exception(ex.Message);
            }    
        }*/

        public async Task<bool> PublishAsync(Event message, string topicName)
        {
            try
            {
                TopicName topic = new(_busSettings.ProjectId, topicName);
                PublisherClient publisher;
                publisher = await PublisherClient.CreateAsync(topic);

                PubsubMessage pubSubMessage = new()
                {
                    Data = ByteString.CopyFrom(JsonConvert.SerializeObject(message), System.Text.Encoding.UTF8)
                };
                if (_busSettings.Token is not null and not "")
                {
                    pubSubMessage.Attributes["token"] = _busSettings.Token;
                }
                string result = await publisher.PublishAsync(pubSubMessage);
                return result != "";
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> PublishAsync(Event[] message, string topicName)
        {
            try
            {
                TopicName topic = new(_busSettings.ProjectId, topicName);
                PublisherClient publisher;
                publisher = await PublisherClient.CreateAsync(topic);

                PubsubMessage pubSubMessage = new()
                {
                    Data = ByteString.CopyFrom(JsonConvert.SerializeObject(message), System.Text.Encoding.UTF8)
                };
                if (_busSettings.Token is not null and not "")
                {
                    pubSubMessage.Attributes["token"] = _busSettings.Token;
                }
                string result = await publisher.PublishAsync(pubSubMessage);
                return result != "";
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}