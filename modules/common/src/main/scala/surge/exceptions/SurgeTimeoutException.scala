// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.exceptions

import java.util.concurrent.TimeoutException

case class SurgeTimeoutException(msg: String) extends TimeoutException(msg)
