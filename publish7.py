import math
import struct
import paho.mqtt.publish as publish
from PyQt5.QtCore import QTimer, QObject
from PyQt5.QtWidgets import QApplication
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class MQTTPublisher(QObject):
    def __init__(self, broker, topics):
        super().__init__()
        self.broker = broker
        self.topics = topics if isinstance(topics, list) else [topics]
        self.count = 1

        # Frequency sweep parameters
        self.freq_start = 1
        self.freq_end = 500
        self.freq_step = 1
        self.frequency = self.freq_start
        self.sweep_direction = 1  # 1 = increasing, -1 = decreasing

        self.amplitude = 1.0
        self.offset = 32768
        self.sample_rate = 4096
        self.time_per_message = 1.0
        self.current_time = 0.0
        self.num_channels = 10  # 5 + 5 interleaved
        self.samples_per_channel = 4096
        self.num_tacho_channels = 2
        self.frame_index = 0

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.publish_message)
        self.timer.start(1000)  # 1 second interval
        logging.debug(f"Initialized MQTTPublisher with broker: {self.broker}, topics: {self.topics}")

    def publish_message(self):
        try:
            # Step 1: Sweep frequency
            self.frequency += self.freq_step * self.sweep_direction
            if self.frequency >= self.freq_end:
                self.frequency = self.freq_end
                self.sweep_direction = -1
            elif self.frequency <= self.freq_start:
                self.frequency = self.freq_start
                self.sweep_direction = 1

            # Recalculate amplitude scaled
            amplitude_scaled = (self.amplitude * 0.5) / (3.3 / 65535)

            # Step 2: Generate sine wave samples
            base_channel_data = []
            for i in range(self.samples_per_channel):
                t = self.current_time + (i / self.sample_rate)
                value = self.offset + amplitude_scaled * math.sin(2 * math.pi * self.frequency * t)
                base_channel_data.append(int(round(value)))

            self.current_time += self.time_per_message

            # Step 3: Interleave for 10 channels
            interleaved = []
            for i in range(self.samples_per_channel):
                for _ in range(self.num_channels):
                    interleaved.append(base_channel_data[i])

            if len(interleaved) != self.samples_per_channel * self.num_channels:
                logging.error("Interleaved data length mismatch")
                return

            # Step 4: Generate tacho data
            tacho_freq_data = [int(self.frequency)] * self.samples_per_channel
            tacho_trigger_data = [0] * self.samples_per_channel
            num_triggers = int(self.frequency)
            if num_triggers > 0:
                step = self.samples_per_channel // num_triggers
                for i in range(num_triggers):
                    index = i * step
                    if index < self.samples_per_channel:
                        tacho_trigger_data[index] = 1

            # Step 5: Build header
            header = [
                self.frame_index % 65535,
                self.frame_index // 65535,
                self.num_channels,
                self.sample_rate,
                4096,
                self.samples_per_channel,
                self.num_tacho_channels,
                0, 0, 0
            ]
            while len(header) < 100:
                header.append(0)

            # Step 6: Combine all parts
            message_values = header + interleaved + tacho_freq_data + tacho_trigger_data
            expected_length = 100 + (self.samples_per_channel * self.num_channels) + (self.samples_per_channel * self.num_tacho_channels)
            if len(message_values) != expected_length:
                logging.error(f"Message length incorrect: expected {expected_length}, got {len(message_values)}")
                return

            # Step 7: Pack into binary
            binary_message = struct.pack(f"<{len(message_values)}H", *message_values)

            # Step 8: Publish to MQTT
            for topic in self.topics:
                try:
                    publish.single(topic, binary_message, hostname=self.broker, qos=1)
                    logging.info(f"[{self.count}] Published to {topic}: frame {self.frame_index}, freq {self.frequency} Hz")
                except Exception as e:
                    logging.error(f"Failed to publish to {topic}: {str(e)}")

            self.frame_index += 1
            self.count += 1

        except Exception as e:
            logging.error(f"Error in publish_message: {str(e)}")


if __name__ == "__main__":
    app = QApplication([])
    broker = "192.168.1.231"  # Replace with your broker IP
    topics = ["sarayu/d1/topic1"]
    mqtt_publisher = MQTTPublisher(broker, topics)
    app.exec_()
