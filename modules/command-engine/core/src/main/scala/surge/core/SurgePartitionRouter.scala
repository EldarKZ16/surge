// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor._
import com.typesafe.config.Config
import play.api.libs.json.JsValue
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.core.SurgePartitionRouterImpl
import surge.kafka.streams._

trait SurgePartitionRouter extends HealthyComponent with Controllable {
  def actorRegion: ActorRef
}

object SurgePartitionRouter {
  def apply(
      config: Config,
      system: ActorSystem,
      businessLogic: SurgeModel[_, _, _, _],
      kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue],
      signalBus: HealthSignalBusTrait): SurgePartitionRouter = {
    new SurgePartitionRouterImpl(config, system, businessLogic, kafkaStreamsCommand, signalBus)
  }
}
