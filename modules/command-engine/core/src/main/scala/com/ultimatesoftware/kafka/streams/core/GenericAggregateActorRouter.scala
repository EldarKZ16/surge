// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor._
import com.ultimatesoftware.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider, Shard }
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck, HealthCheckStatus, HealthyActor, HealthyComponent }
import com.ultimatesoftware.kafka.{ KafkaPartitionShardRouterActor, TopicPartitionRegionCreator }
import com.ultimatesoftware.scala.core.monitoring.metrics.MetricsProvider
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue
import akka.pattern.ask
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.support.Logging
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

private[streams] final class GenericAggregateActorRouter[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    system: ActorSystem,
    clusterStateTrackingActor: ActorRef,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metricsProvider: MetricsProvider,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)

  val actorRegion: ActorRef = {
    val shardRegionCreator = new TopicPartitionRegionCreator {
      override def propsFromTopicPartition(topicPartition: TopicPartition): Props = {
        val provider = new GenericAggregateActorRegionProvider(system, topicPartition, businessLogic,
          kafkaStreamsCommand, metricsProvider)

        Shard.props(topicPartition.toString, provider, GenericAggregateActor.RoutableMessage.extractEntityId)
      }
    }

    val shardRouterProps = KafkaPartitionShardRouterActor.props(clusterStateTrackingActor, businessLogic.partitioner, businessLogic.kafka.stateTopic,
      shardRegionCreator, GenericAggregateActor.RoutableMessage.extractEntityId)
    val actorName = s"${businessLogic.aggregateName}RouterActor"
    system.actorOf(shardRouterProps, name = actorName)
  }

  override def healthCheck(): Future[HealthCheck] = {
    actorRegion
      .ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout * 3)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable ⇒
          log.error(s"Failed to get router-actor health check", err)
          Future.successful(
            HealthCheck(
              name = "router-actor",
              id = s"router-actor-${actorRegion.hashCode}",
              status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}

class GenericAggregateActorRegionProvider[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    system: ActorSystem,
    assignedPartition: TopicPartition,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    metricsProvider: MetricsProvider) extends PerShardLogicProvider[AggId] with Logging {

  val kafkaProducerActor = new KafkaProducerActor[AggId, Agg, Event, EvtMeta](
    actorSystem = system,
    assignedPartition = assignedPartition,
    metricsProvider = metricsProvider,
    aggregateCommandKafkaStreams = businessLogic)

  override def actorProvider(context: ActorContext): EntityPropsProvider[AggId] = {
    val aggregateMetrics = GenericAggregateActor.createMetrics(metricsProvider, businessLogic.aggregateName)

    actorId: AggId ⇒ GenericAggregateActor.props(aggregateId = actorId, businessLogic = businessLogic,
      kafkaProducerActor = kafkaProducerActor, metrics = aggregateMetrics, kafkaStreamsCommand = aggregateKafkaStreamsImpl)
  }

  override def onShardTerminated(): Unit = {
    log.debug("Shard for partition {} terminated, killing partition kafkaProducerActor", assignedPartition)
    kafkaProducerActor.terminate()
  }

  override def healthCheck(): Future[HealthCheck] = {
    kafkaProducerActor.healthCheck()
  }
}
