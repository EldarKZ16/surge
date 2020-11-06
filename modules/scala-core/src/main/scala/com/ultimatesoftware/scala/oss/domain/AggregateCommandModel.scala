// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.oss.domain

trait CommandProcessingException extends Exception
final case class CommandFailedException(msg: String) extends CommandProcessingException
final case class CommandUnauthorizedException(msg: String) extends CommandProcessingException

/**
 * AggregateCommandModel define a structure for a domain's types and algebra
 *
 * Implementations of this trait can be injected into
 *   UltiLagomPersistentEntity
 *   and soon, Surge and perhaps other persistence models (CRUD, Command Sourcing, etc)
 *
 * @tparam Agg  The Aggregate Type
 * @tparam Cmd  The Aggregate's Base Command Type
 * @tparam Evt  The Aggregate's Base Event Type
 */
trait AggregateCommandModel[Agg, Cmd, Evt] {
  def processCommand: CommandProcessor[Agg, Cmd, Evt]
  def handleEvent: EventHandler[Agg, Evt]
}
