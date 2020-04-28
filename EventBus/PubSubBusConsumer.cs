using System;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Newtonsoft.Json;
using System.Threading;
using Google.Api.Gax.Grpc;
using System.Collections.Generic;
using Google.Protobuf.Collections;
using YoungMessaging.Abstractions;
using YoungMessaging.Settings;
using Google.Protobuf;
using System.Text;

namespace YoungMessaging.EventBus
{
    public class PubSubBusConsumer : IBusConsumer, IBusProducer
    {
        private readonly BusSettings _busSettings;
        public PubSubBusConsumer(BusSettings busSettings)
        {
            _busSettings = busSettings;
        }

        public void Subscribe<T, TH>(Func<TH> handler,string topicName, int maxConcurrent = 1)
            where T : Event
            where TH : IEventHandler<T>
        {
            new Thread(async()=>{
                try {
                    string subscription = _busSettings.SubscriptionName.ToLower()+"-"+topicName.ToLower();
                    SubscriberClient subscriber;
                    CreateSubscription(topicName, subscription);
                    // Pull messages from the subscription using SimpleSubscriber.
                    SubscriptionName subscriptionName = new SubscriptionName(_busSettings.ProjectId, subscription);
                    subscriber = await SubscriberClient.CreateAsync(
                        subscriptionName,
                        null,
                        new SubscriberClient.Settings{FlowControlSettings = new Google.Api.Gax.FlowControlSettings(maxConcurrent,null)}
                    );
                
                    await subscriber.StartAsync(async(PubsubMessage message, CancellationToken token)=>{
                            T eventMessage;
                            if((_busSettings.Token != null && _busSettings.Token != "") && (!message.Attributes.ContainsKey("token") || message.Attributes["token"] != _busSettings.Token)){
                                return SubscriberClient.Reply.Ack;
                            }
                            try{
                                eventMessage = JsonConvert.DeserializeObject<T>(message.Data.ToStringUtf8());
                            }catch(JsonException ex){
                                Console.WriteLine(ex.Message);
                                return SubscriberClient.Reply.Ack;
                            }
                            try{ 
                                eventMessage.EventId = message.MessageId;
                                eventMessage.Timestamp = message.PublishTime.Seconds * 1000;
                            }catch(NullReferenceException ex){
                                Console.WriteLine(ex.Message);
                                return SubscriberClient.Reply.Ack;
                            }
                            var invoke = handler.DynamicInvoke();
                            var concreteType = typeof(IEventHandler<>).MakeGenericType(typeof(T));
                            EventResult result = await (Task<EventResult>) concreteType.GetMethod("Handle").Invoke(invoke, new object[] { eventMessage, null });
                            if(result == EventResult.Success)
                                return SubscriberClient.Reply.Ack;
                            else
                                return SubscriberClient.Reply.Nack;
                    });
                    new Thread(()=>Subscribe<T,TH>(handler,topicName, maxConcurrent)).Start();
                } 
                // Restart when connection fail
                catch(RpcException ex)
                {
                    Console.WriteLine(ex.Message);
                    new Thread(()=>Subscribe<T,TH>(handler,topicName, maxConcurrent)).Start();
                    return;
                }
            }).Start();
        }

          public void SubscribeArray<T, TH>(Func<TH> handler,string topicName, int maxConcurrent = 1)
            where T : Event
            where TH : IArrayEventHandler<T>
        {
            new Thread(async()=>{
                try {
                    string subscription = _busSettings.SubscriptionName.ToLower()+"-"+topicName.ToLower();
                    SubscriberClient subscriber;
                    CreateSubscription(topicName, subscription);
                    // Pull messages from the subscription using SimpleSubscriber.
                    SubscriptionName subscriptionName = new SubscriptionName(_busSettings.ProjectId, subscription);
                    subscriber = await SubscriberClient.CreateAsync(
                        subscriptionName,
                        null,
                        new SubscriberClient.Settings{FlowControlSettings = new Google.Api.Gax.FlowControlSettings(maxConcurrent,null)}
                    );
                    
                    await subscriber.StartAsync(async(PubsubMessage message, CancellationToken token)=>{
                            T[] events;
                            if((_busSettings.Token != null && _busSettings.Token != "") && (!message.Attributes.ContainsKey("token") || message.Attributes["token"] != _busSettings.Token)){
                                return SubscriberClient.Reply.Ack;
                            }
                            try{
                                events = JsonConvert.DeserializeObject<T[]>(message.Data.ToStringUtf8());
                            }catch(JsonException ex){
                                Console.WriteLine(ex.Message);
                                return SubscriberClient.Reply.Ack;
                            }
                            for(int i = 0; i < events.Length; i++){
                                events[i].EventId = message.MessageId;
                                events[i].Timestamp = message.PublishTime.Seconds * 1000;
                            }

                            var invoke = handler.DynamicInvoke();
                            var concreteType = typeof(IArrayEventHandler<>).MakeGenericType(typeof(T));
                            EventResult result = await (Task<EventResult>) concreteType.GetMethod("Handle").Invoke(invoke,new object[]{events,null});
                            if(result == EventResult.Success)
                                return SubscriberClient.Reply.Ack;
                            else
                                return SubscriberClient.Reply.Nack;
                    });
                    new Thread(()=>SubscribeArray<T,TH>(handler,topicName,maxConcurrent)).Start();
                } 
                // Restart when connection fail
                catch(RpcException ex)
                {
                    Console.WriteLine(ex.Message);
                    new Thread(()=>SubscribeArray<T,TH>(handler,topicName,maxConcurrent)).Start();
                    return;
                }
            }).Start();
        }

        private void CreateSubscription(string topic, string subscription){
            SubscriberServiceApiClient subscriberService;
            subscriberService = SubscriberServiceApiClient.Create();

            SubscriptionName subscriptionName = new SubscriptionName(_busSettings.ProjectId,subscription);
            TopicName topicName = new TopicName(_busSettings.ProjectId, topic);

            try{
                subscriberService.CreateSubscription(subscriptionName, topicName, pushConfig:null, ackDeadlineSeconds: 20);
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists || ex.StatusCode == StatusCode.Unavailable){
            }
            catch(Exception ex){
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
            try{
                TopicName topic = new TopicName(_busSettings.ProjectId, topicName);
                PublisherClient publisher;
                publisher = await PublisherClient.CreateAsync(topic);
                
                var pubSubMessage = new PubsubMessage{Data= ByteString.CopyFrom(JsonConvert.SerializeObject(message), Encoding.UTF8)};
                if(_busSettings.Token != null && _busSettings.Token != ""){
                    pubSubMessage.Attributes["token"] = _busSettings.Token;
                }
                var result = await publisher.PublishAsync(pubSubMessage);
                if(result == "") {
                    return false;
                }
                return true;
            } catch (Exception ex){
                return false;
            }
        }

        public async Task<bool> PublishAsync(Event[] message, string topicName)
        {
            try{
                TopicName topic = new TopicName(_busSettings.ProjectId, topicName);
                PublisherClient publisher;
                publisher = await PublisherClient.CreateAsync(topic);
                
                var pubSubMessage = new PubsubMessage{Data= ByteString.CopyFrom(JsonConvert.SerializeObject(message), Encoding.UTF8)};
                if(_busSettings.Token != null && _busSettings.Token != ""){
                    pubSubMessage.Attributes["token"] = _busSettings.Token;
                }
                var result = await publisher.PublishAsync(pubSubMessage);
                if(result == "") {
                    return false;
                }
                return true;
            } catch (Exception ex){
                return false;
            }
        }
    }
}