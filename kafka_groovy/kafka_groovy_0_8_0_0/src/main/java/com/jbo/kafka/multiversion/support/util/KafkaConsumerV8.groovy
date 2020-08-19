package com.jbo.kafka.multiversion.support.util

import com.jbo.kafka.multiversion.support.utils.KafkaMultiVersionHelper
import kafka.api.FetchRequest
import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi.FetchResponse
import kafka.javaapi.OffsetRequest
import kafka.javaapi.OffsetResponse
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadata
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.TopicMetadataResponse
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.message.MessageAndOffset
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer

class KafkaConsumerV8<K, V>{
    private Logger logger = LoggerFactory.getLogger(KafkaTest.class)

    private String clientId
    private int bufferSize = 64 * 1024
    private int maxPollSize = 10000     //每次poll最多获取的数量（所有订阅的TopicPartition的fetchSize之和）
    private int fetchSize = 10000     //每次fetch，每一个TopicPartition获取的最多数量
    private String keyDeserializer
    private String valueDeserializer
    private int soTimeout
    private Map<String, Integer> address

    public KafkaConsumerV8(Properties props) {
        this.clientId = props.getProperty("client.id")
        this.maxPollSize = StringUtils.isNumeric(String.valueOf(props.get("max.poll.records"))) ? Integer.parseInt(String.valueOf(props.get("max.poll.records"))) : 1000
        String bootstrapServers = props.getProperty("bootstrap.servers")
        this.address = KafkaMultiVersionHelper.transferAddress(bootstrapServers)
        this.keyDeserializer = props.getProperty("key.deserializer")
        this.valueDeserializer = props.getProperty("value.deserializer")
        this.soTimeout = StringUtils.isNumeric(String.valueOf(props.get("session.timeout.ms"))) ? Integer.parseInt(String.valueOf(props.get("session.timeout.ms"))) : 1000
    }

    private Map<TopicPartition, PartitionMetadata> topicPartitionMetadatas = new HashMap<>()
    private Map<TopicPartition, SimpleConsumer> topicPartitionConsumer = new HashMap<>()
    private Map<TopicPartition, Long> topicPartitionOffset = new HashMap<>()

    public void assign(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            PartitionMetadata partitionMetadata = findLeader(address, topicPartition)
            if (partitionMetadata != null) {
                logger.info(String.format("topic:%s partition:%d 的 leader 为 %s:%d", topicPartition.topic(), topicPartition.partition(), partitionMetadata.leader().host(), partitionMetadata.leader().port()))
                topicPartitionMetadatas.put(topicPartition, partitionMetadata)
                Broker leaderBroker = partitionMetadata.leader()
                SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), soTimeout, bufferSize, clientId)
                topicPartitionConsumer.put(topicPartition, simpleConsumer)
                topicPartitionOffset.put(topicPartition, 0L)
                logger.info(String.format("订阅的topic:%s partition:%d ", topicPartition.topic(), topicPartition.partition()))
            }
        }
    }

    public void seek(TopicPartition topicPartition, Long offset) {
        SimpleConsumer simpleConsumer = topicPartitionConsumer.get(topicPartition)
        long actualOffset = getActualOffset(simpleConsumer, topicPartition, kafka.api.OffsetRequest.EarliestTime(), clientId)
        //logger.info(String.format("topic:%s , partition:%d ,本次开始消费的offset:%d，kafka中的offset:%d",topicPartition.topic(),topicPartition.partition(),offset,actualOffset))
        topicPartitionOffset.put(topicPartition,offset)
        if(offset > actualOffset){
            logger.warn(String.format("topic:%s partition:%d 的offset:%d 大于kafka实际的offset:%d", topicPartition.topic(), topicPartition.partition(), offset, actualOffset))
        }
        topicPartitionOffset.put(topicPartition,offset)
        logger.info(String.format("设置topic:%s partition:%d 的offset:%d", topicPartition.topic(), topicPartition.partition(), offset))
    }

    public void subscribe(List<String> topics) {
        Map<String, List<PartitionInfo>> topicMap = listTopics()
        for(String topic : topics){
            if(topicMap.containsKey(topic)) {
                List<PartitionInfo> partitionInfos = topicMap.get(topic)
                if(partitionInfos == null) {
                    logger.info(String.format("topic：%s不存在", topic))
                } else{
                    for(PartitionInfo partitionInfo : partitionInfos){
                        TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(),partitionInfo.partition())
                        PartitionMetadata partitionMetadata = findLeader(address,topicPartition)
                        logger.info(String.format("topic:%s partition:%d 的 leader 为 %s:%d", topicPartition.topic(), topicPartition.partition(), partitionMetadata.leader().host(), partitionMetadata.leader().port()))
                        topicPartitionMetadatas.put(topicPartition,partitionMetadata)
                        Broker leaderBroker = partitionMetadata.leader()
                        SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), soTimeout, bufferSize, clientId)
                        topicPartitionConsumer.put(topicPartition,simpleConsumer)
                        topicPartitionOffset.put(topicPartition,0L)
                        logger.info(String.format("订阅的topic:%s partition:%d ", topicPartition.topic(), topicPartition.partition()))
                    }
                }
            } else{
                logger.error(String.format("subscribe topic:%s不存在", topic))
            }
        }
    }

    public Set<TopicPartition> assignment() {
        return topicPartitionMetadatas.keySet()
    }


    public Map<String, List<PartitionInfo>> listTopics() {
        Map<String, List<PartitionInfo>> topics = new HashMap<>()
        for(String host : address.keySet()){
            int port = address.get(host)
            List<String> ts = new ArrayList<>()
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ts)
            SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId)
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest)
            List<TopicMetadata> topicMetaDataList = topicMetadataResponse.topicsMetadata()
            topics.putAll(toPartitionInfo(topicMetaDataList))
        }
        return topics
    }

    public void close() {
        for(SimpleConsumer simpleConsumer : topicPartitionConsumer.values()){
            simpleConsumer.close()
        }
    }

    public List<ConsumerRecord<K, V>> poll(Long timeout) {
        List<ConsumerRecord<K, V>> consumerRecordList = new ArrayList<>()
        int onePollSize = 0
        long time = System.currentTimeMillis()
        while(timeout > (System.currentTimeMillis() - time)){
            for(TopicPartition topicPartition : topicPartitionMetadatas.keySet()){
                PartitionMetadata partitionMetadata = topicPartitionMetadatas.get(topicPartition)
                if(partitionMetadata == null){
                    topicPartitionMetadatas.put(topicPartition, findLeader(address,topicPartition))
                } else{
                    long currentReadOffset = topicPartitionOffset.get(topicPartition)
                    SimpleConsumer simpleConsumer = topicPartitionConsumer.get(topicPartition)
                    FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topicPartition.topic(), topicPartition.partition(), currentReadOffset, fetchSize).build()
                    FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest)
                    if(fetchResponse.hasError()){
                        handlerPollErrorCode(partitionMetadata, topicPartition, currentReadOffset, simpleConsumer, fetchResponse)
                    } else{
                        boolean continueObtainFlag = true
                        ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topicPartition.topic(), topicPartition.partition())
                        for(MessageAndOffset messageAndOffset : byteBufferMessageSet){
                            if(continueObtainFlag){
                                long currentOffset = messageAndOffset.offset()
                                if(currentOffset >= currentReadOffset){
                                    handlerMessageToConsumerRecord(messageAndOffset, topicPartition, currentOffset, consumerRecordList)
                                    currentReadOffset = messageAndOffset.nextOffset()
                                    if( ++onePollSize >= maxPollSize){
                                        continueObtainFlag = false
                                    }
                                }
                            }
                        }
                    }
                    topicPartitionOffset.put(topicPartition, currentReadOffset)
                }
            }
        }
        return consumerRecordList
    }

    public List<V> polls(Long timeout) {
        List<V> values = new ArrayList<>()
        int onePollSize = 0
        long time = System.currentTimeMillis()
        while(timeout > (System.currentTimeMillis() - time)){
            for(TopicPartition topicPartition : topicPartitionMetadatas.keySet()){
                PartitionMetadata partitionMetadata = topicPartitionMetadatas.get(topicPartition)
                boolean continueObtainFlag = true
                if(partitionMetadata == null){
                    topicPartitionMetadatas.put(topicPartition, findLeader(address,topicPartition))
                } else{
                    long currentReadOffset = topicPartitionOffset.get(topicPartition)
                    SimpleConsumer simpleConsumer = topicPartitionConsumer.get(topicPartition)
                    FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topicPartition.topic(), topicPartition.partition(), currentReadOffset, fetchSize).build()
                    FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest)
                    if(fetchResponse.hasError()){
                        handlerPollErrorCode(partitionMetadata, topicPartition, currentReadOffset, simpleConsumer, fetchResponse)
                    } else{
                        ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topicPartition.topic(), topicPartition.partition())
                        for(MessageAndOffset messageAndOffset : byteBufferMessageSet ){
                            if(continueObtainFlag){
                                long currentOffset = messageAndOffset.offset()
                                if(currentOffset >= currentReadOffset){
                                    handlerMessageToConsumerRecord(messageAndOffset, topicPartition, currentOffset, values)
                                    currentReadOffset = messageAndOffset.nextOffset()
                                    if( ++onePollSize >= maxPollSize){
                                        continueObtainFlag = false
                                    }
                                }
                            }
                        }
                    }
                    topicPartitionOffset.put(topicPartition, currentReadOffset)
                }
            }
        }
        return consumerRecordList
    }

    private void handlerPollErrorCode(PartitionMetadata partitionMetadata, TopicPartition topicPartition, long offset, SimpleConsumer simpleConsumer, FetchResponse fetchResponse){
        Broker leaderBroker = partitionMetadata.leader()
        Short errorCode = fetchResponse.errorCode(topicPartition.topic(),topicPartition.partition())
        //重设offset
        if(errorCode == ErrorMapping.OffsetOutOfRangeCode()){
            long actualOffset = getActualOffset(simpleConsumer, topicPartition, kafka.api.OffsetRequest.EarliestTime(), clientId)
            logger.warn(String.format("topic:%s partition:%d 的offset:%d 大于kafka实际的offset:%d,offset将设置为%d", topicPartition.topic(), topicPartition.partition(), offset, actualOffset, actualOffset))
            //topicPartitionOffset.put(topicPartition,0L)
            topicPartitionOffset.put(topicPartition, actualOffset)
        }else{//重新选举leader,读取数据
            logger.error("Error fetching data from the Broker:" + leaderBroker + " Reason: " + errorCode)
            PartitionMetadata newPartitionMetadata = findNewLeader(leaderBroker.host(), topicPartition)
            topicPartitionMetadatas.put(topicPartition, newPartitionMetadata)
        }
    }

    private void handlerMessageToConsumerRecord(MessageAndOffset messageAndOffset, TopicPartition topicPartition, long currentOffset, List<ConsumerRecord<K, V>> consumerRecordList){
        ByteBuffer payload = messageAndOffset.message().payload()
        byte[] bytes = new byte[payload.limit()]
        payload.get(bytes)
        try{
            Object value = getDeserializer(valueDeserializer, messageAndOffset.message().payload())
            if(logger.isDebugEnabled()){
                logger.debug(topicPartition.topic() + "  " + currentOffset + ": "  + ":" + value.getClass() + ":" + new String(bytes, "UTF-8"))
            }
            if(messageAndOffset.message().hasKey()){
                Object key = getDeserializer(keyDeserializer, messageAndOffset.message().key())
                ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(topicPartition.topic(),topicPartition.partition(), (K)key, (V)value, currentOffset)
                consumerRecordList.add(consumerRecord)
            }else{
                ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), (V)value, currentOffset)
                consumerRecordList.add(consumerRecord)
            }
        } catch (Exception e){
            logger.error("读取kafka数据失败", e)
        }
    }

    private void handlerMessageToValues(MessageAndOffset messageAndOffset, TopicPartition topicPartition, long currentOffset, List<V> values){
        ByteBuffer payload = messageAndOffset.message().payload()
        byte[] bytes = new byte[payload.limit()]
        payload.get(bytes)
        try{
            Object value = getDeserializer(valueDeserializer, messageAndOffset.message().payload())
            if(logger.isDebugEnabled()){
                logger.debug(topicPartition.topic() + "  " + currentOffset + ": "  + ":" + value.getClass() + ":" + new String(bytes, "UTF-8"))
            }
            values.add((V)value)
        } catch (Exception e){
            logger.error("读取kafka数据失败", e)
        }
    }

    private PartitionMetadata findLeader(Map<String, Integer> addresses, TopicPartition topicPartition){
        for(String host : addresses.keySet()){
            Integer port = addresses.get(host)
            List<String> topics = Collections.singletonList(topicPartition.topic())
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics)
            SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId)
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest)
            List<TopicMetadata> topicMetaDatas = topicMetadataResponse.topicsMetadata()
            for(TopicMetadata topicMetadata : topicMetaDatas){
                for(PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()){
                    if(partitionMetadata.partitionId() == topicPartition.partition()){
                        return partitionMetadata
                    }
                }
            }
        }
        logger.error(String.format("topic:%s partition:%d 所在的leader未发现", topicPartition.topic(), topicPartition.partition()))
        return null
    }


    private PartitionMetadata findNewLeader(String oldLeaderHost, TopicPartition topicPartition) {
        logger.info("开始从新选举leader")
        for(int i=0;i<3;i++){
            PartitionMetadata partitionMetadata = topicPartitionMetadatas.get(topicPartition)
            HashMap<String, Integer> hostPort = new HashMap<>()
            for(Broker broker : partitionMetadata.replicas()){
                hostPort.put(broker.host(), broker.port())
            }
            PartitionMetadata newPartitionMetadata = findLeader(hostPort,topicPartition)
            if(newPartitionMetadata == null || newPartitionMetadata.leader() == null || (oldLeaderHost.equalsIgnoreCase(newPartitionMetadata.leader().host()) && i == 0)){
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                logger.error("Unable to find new leader after Broker failure. Exiting")
            }else{
                logger.info(String.format("topic:%s partition:%d 的 leader 为 %s:%d", topicPartition.topic(), topicPartition.partition(), partitionMetadata.leader().host(), partitionMetadata.leader().port()))
                return newPartitionMetadata
            }
            try{
                Thread.sleep((i + 1)*1000)
            }
            catch(InterruptedException e){
                logger.error("", e)
            }
        }
        return null
    }

    private long getActualOffset(SimpleConsumer simpleConsumer,TopicPartition topicPartition,Long whichTime,String clientId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicPartition.topic(),topicPartition.partition())
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>()
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime,1))
        OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(),clientId)
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request)
        long offset = 0L
        if(response.hasError()){
            logger.error(String.format("topic:%s partition:%d 获取offset失败,Reason: %d", topicPartition.topic(), topicPartition.partition(), response.errorCode(topicPartition.topic(), topicPartition.partition())))
        }else{
            long[] offsets = response.offsets(topicPartition.topic(), topicPartition.partition())
            offset =  offsets[0]
        }
        return offset
    }

    private Map<String, List<PartitionInfo>> toPartitionInfo(List<TopicMetadata> topicMetadataList) {
        Map<String, List<PartitionInfo>> topics = new HashMap<>()
        for(TopicMetadata topicMetadata : topicMetadataList){
            String topic = topicMetadata.topic()
            List<PartitionMetadata> partitionsMetadataList = topicMetadata.partitionsMetadata()
            List<PartitionInfo> partitionInfos = new ArrayList<>()
            for(PartitionMetadata partitionMetadata : partitionsMetadataList){
                Integer partitionId = partitionMetadata.partitionId()
                Node leader = brokerToNode(partitionMetadata.leader())
                Node[] replicas = brokerToNode(partitionMetadata.replicas())
                Node[] inSyncReplicas = brokerToNode(partitionMetadata.isr())
                PartitionInfo partitionInfo = new PartitionInfo(topic, partitionId, leader, replicas, inSyncReplicas)
                partitionInfos.add(partitionInfo)
            }
            topics.put(topic, partitionInfos)

        }
        return topics
    }

    private Node brokerToNode(Broker broker) {
        return new Node(broker.id(), broker.host(), broker.port())
    }

    private Node[] brokerToNode(List<Broker> brokers) {
        Node[] nodes = new Node[brokers.size()]
        for(int i=0;i<brokers.size();i++){
            nodes[i] = brokerToNode(brokers.get(i))
        }
        return nodes
    }

    private Object getDeserializer(String valueType, ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.limit()]
        byteBuffer.get(bytes)
        def value
        switch (valueType) {
            case "StringDeserializer" :
                value = new String(bytes)
                break
            case "ByteBufferDeserializer":
                value = ByteBuffer.wrap(bytes)
                break
            case "IntegerDeserializer" :
                value = Integer.parseInt(new String(bytes))
                break
            case "LongDeserializer" :
                value = Long.parseLong(new String(bytes))
                break
            case "DoubleDeserializer" :
                value = Double.parseDouble(new String(bytes))
                break
            default:
                value = bytes
        }
        return value
    }

}
