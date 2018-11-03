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

        public void Subscribe<T, TH>(Func<TH> handler)
            where T : Event
            where TH : IEventHandler<T>
        {
            new Thread(()=>{
                try {
                    var topicName = typeof(T).Name.ToLower();
                    string subscription = _busSettings.SubscriptionName.ToLower()+"-"+topicName.ToLower();
                    SubscriberServiceApiClient subscriberService;
                    if(_busSettings.BusHost != null && _busSettings.BusHost != ""){
                        Channel channel = new Channel(_busSettings.BusHost+":"+_busSettings.BusPort,ChannelCredentials.Insecure);
                        subscriberService = SubscriberServiceApiClient.Create(channel);
                    }
                    else {
                        subscriberService = SubscriberServiceApiClient.Create();
                    }
                    CreateSubscription(subscriberService,topicName, subscription);
                    // Pull messages from the subscription using SimpleSubscriber.
                    SubscriptionName subscriptionName = new SubscriptionName(_busSettings.ProjectId, subscription);
                    SubscriberServiceApiClient.StreamingPullStream stream = subscriberService.StreamingPull();

                    // The first request must include the subscription name and the stream ack deadline
                    stream.WriteAsync(new StreamingPullRequest { SubscriptionAsSubscriptionName = subscriptionName, StreamAckDeadlineSeconds = 20 }).GetAwaiter().GetResult();

                    IAsyncEnumerator<StreamingPullResponse> responseStream = stream.ResponseStream;

                    // Handle responses as we see them.
                    while (responseStream.MoveNext().GetAwaiter().GetResult())
                    {
                        StreamingPullResponse response = responseStream.Current;
                        RepeatedField<string> successIds = new RepeatedField<string>();
                        foreach (ReceivedMessage message in response.ReceivedMessages)
                        {
                            T eventMessage;
                            try{
                                eventMessage = JsonConvert.DeserializeObject<T>(message.Message.Data.ToStringUtf8());
                            }catch(JsonException ex){
                                Console.WriteLine(ex.Message);
                                continue;
                            } 
                            eventMessage.EventId = message.Message.MessageId;
                            eventMessage.Timestamp = message.Message.PublishTime.Seconds * 1000;
                            var invoke = handler.DynamicInvoke();
                            var concreteType = typeof(IEventHandler<>).MakeGenericType(typeof(T));

                            var resultTask = (Task<EventResult>) concreteType.GetMethod("Handle").Invoke(invoke, new object[] { eventMessage, null });
                            EventResult result = resultTask.GetAwaiter().GetResult();
                            if(result == EventResult.Success)
                                successIds.Add(message.AckId);
                        }
                        // Acknowledge the messages we've just seen
                        stream.WriteAsync(new StreamingPullRequest { AckIds = { successIds } }).GetAwaiter().GetResult();
                    }
                } 
                // Restart when connection fail
                catch(RpcException ex)
                {
                    Console.WriteLine(ex.Message);
                    new Thread(()=>Subscribe<T,TH>(handler)).Start();
                    return;
                }
            }).Start();
        }

        private void CreateSubscription(SubscriberServiceApiClient subscriberService, string topic, string subscription){


            SubscriptionName subscriptionName = new SubscriptionName(_busSettings.ProjectId,subscription);
            TopicName topicName = new TopicName(_busSettings.ProjectId, topic);
            CreateTopic(topic);
            try{
            subscriberService.CreateSubscription(subscriptionName, topicName, pushConfig:null, ackDeadlineSeconds: 20);
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists){
            }
            catch(Exception ex){
                throw new Exception(ex.Message);
            }
        }

        private void CreateTopic(string topic){
            PublisherServiceApiClient publisherService;
            
            if(_busSettings.BusHost != null && _busSettings.BusHost != ""){
                Channel channel = new Channel(_busSettings.BusHost+":"+_busSettings.BusPort,ChannelCredentials.Insecure);
                publisherService = PublisherServiceApiClient.Create(channel);
            }
            else {
                publisherService = PublisherServiceApiClient.Create();
            }

            TopicName topicName = new TopicName(_busSettings.ProjectId, topic);

            try{
                publisherService.CreateTopic(topicName,new Google.Api.Gax.Grpc.CallSettings(null,null,CallTiming.FromTimeout(new TimeSpan(0,0,5)),null,null,null));
            }
            catch(RpcException ex)
            when(ex.StatusCode == StatusCode.AlreadyExists){
            }
            catch(Exception ex){
                throw new Exception(ex.Message);
            }    
        }

        public async Task<bool> PublishAsync(Event message)
        {
            var topicName = message.GetType().ToString().ToLower();
            string subscription = _busSettings.SubscriptionName.ToLower()+"-"+topicName.ToLower();
            PublisherServiceApiClient publisherService;
            if(_busSettings.BusHost != null && _busSettings.BusHost != ""){
                Channel channel = new Channel(_busSettings.BusHost+":"+_busSettings.BusPort,ChannelCredentials.Insecure);
                publisherService = PublisherServiceApiClient.Create(channel);
            }
            else {
                publisherService = PublisherServiceApiClient.Create();
            }
            CreateTopic(topicName);
            RepeatedField<PubsubMessage> messages = new RepeatedField<PubsubMessage>();
            messages.Add(new PubsubMessage{Data= ByteString.CopyFrom(JsonConvert.SerializeObject(message), Encoding.UTF8)});
            var result = await publisherService.PublishAsync(new TopicName(_busSettings.ProjectId, topicName),messages);
            if(result.MessageIds.Count <= 0) {
                return false;
            }
            return true;
        }
        
    }
}