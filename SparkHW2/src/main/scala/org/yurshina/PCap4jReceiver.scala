package org.yurshina

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode
import org.pcap4j.core.Pcaps
import org.pcap4j.packet.{TcpPacket, EthernetPacket, IpV4Packet}

case class NetworkPacket(ip: String, lengthBytes: Int) extends Serializable

class PCap4jReceiver() extends Receiver[NetworkPacket](StorageLevel.MEMORY_AND_DISK_2) {

  val PCAP_4J_SNAPLEN_BYTES = 1024
  val PCAP_4J_TIMEOUT_SEC = 5
  val PCAP_4J_DEVICE = "wlan0"

  override def onStart() {
    val worker = new Thread(new Runnable() {
      override def run() {
        runSniffer()
      }
    }, "PCap4j daemon thread")

    worker.setDaemon(true)
    worker.start()
  }

  private def runSniffer(): Unit = {
    val nif = Pcaps.getDevByName(PCAP_4J_DEVICE)

    val handle = nif.openLive(PCAP_4J_SNAPLEN_BYTES, PromiscuousMode.PROMISCUOUS, PCAP_4J_TIMEOUT_SEC)

    try {
      while (!isStopped()) {
        val packet = handle.getNextPacket

        if (packet.isInstanceOf[EthernetPacket]) {
          packet.getPayload match {
            case innerIP: IpV4Packet =>
              innerIP.getPayload match {
                case innerTCP: TcpPacket =>
                  store(new NetworkPacket(innerIP.getHeader.getDstAddr.getHostAddress,
                    innerIP.getHeader.getTotalLength)
                  )
                case _ =>
              }
            case _ =>
          }
        }
      }
    } finally
      handle.close()
  }

  override def onStop() {
    // nothing
  }
}