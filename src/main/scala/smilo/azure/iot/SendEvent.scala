package smilo.azure.iot

import com.microsoft.azure.sdk.iot.device._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

// Original Java source is here,
// https://github.com/Azure/azure-iot-sdk-java/blob/master/device/iot-device-samples/send-event/src/main/java/samples/com/microsoft/azure/sdk/iot/SendEvent.java

/** Sends a number of event messages to an IoT Hub. */
object SendEvent {

  private val D2C_MESSAGE_TIMEOUT = 2000 // 2 seconds
  private val failedMessageListOnClose = new ListBuffer[String]()

  class EventCallback extends IotHubEventCallback {
    override def execute(responseStatus: IotHubStatusCode, callbackContext: scala.Any): Unit = {
      val msg = callbackContext.asInstanceOf[Message]
      println(s"IoT Hub responded to message ${msg.getMessageId} with status ${responseStatus.name()}")
      if (responseStatus == IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE) {
        failedMessageListOnClose += msg.getMessageId
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println("Starting...")
    println("Beginning  setup.")

    if (args.length <= 1 || args.length >= 5) {
      println(s"Expected 2 or 3 arguments but received: ${args.length}.\n"
        + "The program should be called with the following args: \n"
        + "1. [Device connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
        + "2. [number of requests to send]\n"
        + "3. (mqtt | https | amqps | amqps_ws)\n"
        + "4. (optional) path to certificate to enable one-way authentication over ssl for amqps \n")
      System.exit(0)
    }

    val connString = args(0)
    val numRequests = Try(Integer.parseInt(args(1)))
    val protocol =
      if (args.length == 2) Some(IotHubClientProtocol.MQTT)
      else args(2) match {
        case "https" => Some(IotHubClientProtocol.HTTPS)
        case "amqps" => Some(IotHubClientProtocol.AMQPS)
        case "mqtt" => Some(IotHubClientProtocol.MQTT)
        case "amqps_ws" => Some(IotHubClientProtocol.AMQPS_WS)
        case _ => None
      }
    val pathToCertificate = if (args.length < 3) None else Some(args(3))

    (numRequests, protocol) match {
      case (Failure(err), _) =>
        println(s"Could not parse the number of requests to send. "
          + s"Expected an int but received:\n ${args(1)}.")

      case (_, None) =>
        println(s"Expected argument 2 to be one of 'mqtt', 'https', 'amqps' or 'amqps_ws' but received ${args(2)}\n"
          + "The program should be called with the following args: \n"
          + "1. [Device connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
          + "2. [number of requests to send]\n"
          + "3. (mqtt | https | amqps | amqps_ws)\n"
          + "4. (optional) path to certificate to enable one-way authentication over ssl for amqps")

      case (Success(numReq), Some(p)) =>
        println("Successfully read input parameters.")
        println(s"Using communication protocol ${protocol.getOrElse("UNKNOWN")}")

        val client = new DeviceClient(connString, p)
        if (pathToCertificate.nonEmpty)
          client.setOption("SetCertificatePath", pathToCertificate.get)

        println("Successfully created an IoT Hub client.")

        // Set your token expiry time limit here
        val time = 2400L
        client.setOption("SetSASTokenExpiryTime", time)
        println(s"Updated token expiry time to $time")

        client.open()

        println("Opened connection to IoT Hub.")
        println("Sending the following event messages:")

        val deviceId = "MyScalaDevice"

        for (i <- 0 until numReq) {
          val temperature = 20 + Math.random() * 10
          val humidity = 30 + Math.random() * 20
          val msgStr = "{\"deviceId\":\"" + deviceId +"\",\"messageId\":" + i + ",\"temperature\":"+ temperature +",\"humidity\":"+ humidity +"}"

          Try {
            val msg = new Message(msgStr)
            msg.setProperty("temperatureAlert", if (temperature > 28) "true" else "false")
            msg.setMessageId(java.util.UUID.randomUUID().toString)
            msg.setExpiryTime(D2C_MESSAGE_TIMEOUT)
            println(msgStr)

            val callback = new EventCallback()
            client.sendEventAsync(msg, callback, msg)
          }
        }

        println(s"Wait for ${D2C_MESSAGE_TIMEOUT / 1000} second(s) for response from the IoT Hub...")
        Try {
          Thread.sleep(D2C_MESSAGE_TIMEOUT)
        }

        println("Closing")
        client.closeNow()

        if (failedMessageListOnClose.nonEmpty)
          println(s"List of messages that were cancelled on close: ${failedMessageListOnClose.toList.toString()}")

        println("Shutting down...")
    }
  }
}
