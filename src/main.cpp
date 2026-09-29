#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <ArduinoJson.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <esp_sntp.h>

// =============================================================================
// CẤU HÌNH KẾT NỐI WI-FI VÀ MQTT BROKER
// =============================================================================
const char* WIFI_SSID     = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

// MQTT Broker: dùng Mosquitto public hoặc HiveMQ Cloud
// Nếu dùng local broker: sửa thành IP của máy (VD: "192.168.1.100")
const char* MQTT_SERVER = "broker.hivemq.com";
const int   MQTT_PORT   = 1883;

// Topic chuẩn theo cấu trúc: iot/<student_id>/<device_type>/telemetry
const char* MQTT_TOPIC_TELEMETRY = "iot/B23DCAT076/esp32/telemetry";
const char* MQTT_TOPIC_STATUS    = "iot/B23DCAT076/esp32/status";

// =============================================================================
// CẤU HÌNH CHÂN GPIO VÀ THỜI GIAN
// =============================================================================
const int DHT_PIN   = 15;   // Cảm biến DHT22 nối GPIO 15
const int LDR_PIN   = 34;   // Cảm biến ánh sáng LDR nối GPIO 34 (ADC)
const int LED_PIN   = 2;    // LED trạng thái nối GPIO 2

const unsigned long SEND_INTERVAL_MS       = 5000;  // Chu kỳ gửi: 5 giây
const unsigned long MQTT_RETRY_INTERVAL_MS = 3000;  // Thử kết nối lại MQTT: 3 giây

// =============================================================================
// KHỞI TẠO ĐỐI TƯỢNG VÀ BIẾN TOÀN CỤC
// =============================================================================
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
DHTesp dht;

unsigned long lastSend        = 0;
unsigned long lastMqttAttempt = 0;
unsigned long sequenceNo      = 0;
volatile bool ntpSynced = false;
volatile unsigned long lastNtpSyncMs = 0;
volatile uint32_t ntpSyncCount = 0;
volatile bool ntpSyncLogPending = false;

void onTimeSync(struct timeval *tv) {
  lastNtpSyncMs = millis();
  ntpSyncCount++;
  ntpSynced = true;
  ntpSyncLogPending = true;
}

// =============================================================================
// HÀM KẾT NỐI WI-FI
// =============================================================================
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;

  Serial.println();
  Serial.print("[WI-FI] Dang ket noi mang: ");
  Serial.println(WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);

  unsigned long startAttempt = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startAttempt < 15000) {
    delay(300);
    Serial.print(".");
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println();
    Serial.println("[WI-FI] DA KET NOI THANH CONG!");
    Serial.print("[WI-FI] Dia chi IP: ");
    Serial.println(WiFi.localIP());
    Serial.print("[WI-FI] RSSI: ");
    Serial.print(WiFi.RSSI());
    Serial.println(" dBm");
  } else {
    Serial.println();
    Serial.println("[WI-FI] Ket noi that bai. Thu lai sau...");
  }
}

// =============================================================================
// HÀM KẾT NỐI MQTT
// =============================================================================
bool connectMQTT() {
  if (mqttClient.connected()) return true;
  if (WiFi.status() != WL_CONNECTED) return false;

  unsigned long now = millis();
  if (now - lastMqttAttempt < MQTT_RETRY_INTERVAL_MS) return false;
  lastMqttAttempt = now;

  String clientId = "ESP32-Bai2-" + String((uint32_t)ESP.getEfuseMac(), HEX);

  Serial.println();
  Serial.printf("[MQTT] Dang ket noi Broker %s:%d ...\n", MQTT_SERVER, MQTT_PORT);

  if (mqttClient.connect(clientId.c_str())) {
    Serial.println("[MQTT] KET NOI BROKER THANH CONG!");

    // Publish trạng thái online
    StaticJsonDocument<128> statusDoc;
    statusDoc["status"] = "online";
    statusDoc["device_id"] = clientId;
    char statusBuf[128];
    serializeJson(statusDoc, statusBuf);
    mqttClient.publish(MQTT_TOPIC_STATUS, statusBuf, true);

    return true;
  } else {
    int rc = mqttClient.state();
    Serial.printf("[MQTT] KET NOI THAT BAI, ma loi (rc = %d)\n", rc);
    return false;
  }
}

// =============================================================================
// HÀM ĐỌC DHT22 DỰ PHÒNG
// =============================================================================
bool readDHT22Direct(float &tempOut, float &humOut) {
  pinMode(DHT_PIN, OUTPUT);
  digitalWrite(DHT_PIN, LOW);
  delayMicroseconds(2000);
  pinMode(DHT_PIN, INPUT_PULLUP);
  delayMicroseconds(30);

  if (pulseIn(DHT_PIN, HIGH, 250) == 0) return false;

  uint8_t data[5] = {0, 0, 0, 0, 0};
  for (int i = 0; i < 40; i++) {
    unsigned long highDuration = pulseIn(DHT_PIN, HIGH, 150);
    if (highDuration == 0) return false;
    data[i / 8] <<= 1;
    if (highDuration > 45) {
      data[i / 8] |= 1;
    }
  }

  if (data[4] != ((data[0] + data[1] + data[2] + data[3]) & 0xFF)) {
    return false;
  }

  humOut  = (float)((data[0] << 8) | data[1]) * 0.1f;
  int16_t rawTemp = (int16_t)(((data[2] & 0x7F) << 8) | data[3]);
  if (data[2] & 0x80) rawTemp = -rawTemp;
  tempOut = (float)rawTemp * 0.1f;
  return true;
}

// =============================================================================
// HÀM ĐỌC CẢM BIẾN ÁNH SÁNG (LDR)
// =============================================================================
float readLightLevel() {
  int rawADC = analogRead(LDR_PIN);
  // Chuyển đổi ADC 12-bit (0-4095) sang phần trăm ánh sáng (0-100%)
  float lightPercent = (float)rawADC / 4095.0f * 100.0f;
  return lightPercent;
}

// =============================================================================
// SETUP
// =============================================================================
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("================================================================");
  Serial.println("  BAI THUC HANH 2: THU THAP, LUU TRU VA TIEN XU LY DU LIEU IoT");
  Serial.println("================================================================");

  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  pinMode(DHT_PIN, INPUT_PULLUP);
  dht.setup(DHT_PIN, DHTesp::DHT22);

  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  mqttClient.setBufferSize(768);

  sntp_set_time_sync_notification_cb(onTimeSync);
  connectWiFi();
  // Epoch stays UTC. Never add Vietnam's UTC+7 to gettimeofday().
  sntp_set_sync_interval(15000);
  configTime(0, 0, "time.cloudflare.com", "pool.ntp.org", "time.google.com");
  Serial.println("[CLOCK] Dang dong bo UTC qua NTP...");
  unsigned long syncStart = millis();
  while (!ntpSynced && millis() - syncStart < 20000) delay(100);
  Serial.println(ntpSynced
    ? "[CLOCK] NTP SYNCED - san sang do latency (may tinh cung can dong bo gio)."
    : "[CLOCK] NTP chua dong bo; tam dung publish telemetry, tiep tuc cho NTP.");
}

// =============================================================================
// LOOP CHÍNH
// =============================================================================
void loop() {
  if (ntpSyncLogPending) {
    ntpSyncLogPending = false;
    struct timeval syncedTime;
    gettimeofday(&syncedTime, nullptr);
    Serial.printf("[CLOCK] NTP SYNCED count=%lu | unix_epoch_ms=%llu | timezone_offset=0\n",
      (unsigned long)ntpSyncCount,
      (unsigned long long)syncedTime.tv_sec * 1000ULL + syncedTime.tv_usec / 1000);
  }
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }

  if (!mqttClient.connected()) {
    connectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastSend < SEND_INTERVAL_MS) {
    delay(10);
    return;
  }
  lastSend = now;

  // 1. Đọc cảm biến DHT22
  TempAndHumidity dhtData = dht.getTempAndHumidity();
  float temp = dhtData.temperature;
  float hum  = dhtData.humidity;

  if (isnan(temp) || isnan(hum) || !isfinite(temp) || !isfinite(hum)) {
    readDHT22Direct(temp, hum);
  }

  // 2. Đọc cảm biến ánh sáng LDR
  float light = readLightLevel();

  // 3. Đọc RSSI và tính uptime
  long rssi = WiFi.RSSI();
  unsigned long uptime = now / 1000UL;

  // Hiển thị lên Serial Monitor
  Serial.println();
  Serial.println("---------------------------------------------------------------");
  Serial.printf("[THOI DIEM] Uptime: %lu s | Chu ky gui thu: #%lu\n", uptime, sequenceNo + 1);
  Serial.printf("[CAM BIEN] Nhiet do: %.2f C | Do am: %.2f %%RH | Anh sang: %.1f %%\n",
                temp, hum, light);

  // 4. Kiểm tra tính hợp lệ
  if (isnan(temp) || isnan(hum) || !isfinite(temp) || !isfinite(hum)) {
    Serial.println("[CANH BAO] Cam bien tra ve NaN! Bo qua chu ky nay.");
    return;
  }

  // 5. Đóng gói JSON bằng ArduinoJson
  StaticJsonDocument<512> doc;
  doc["temperature"]  = round(temp * 100.0) / 100.0;
  doc["humidity"]     = round(hum * 100.0) / 100.0;
  doc["light_level"]  = round(light * 10.0) / 10.0;
  doc["rssi"]         = rssi;
  doc["uptime_s"]     = uptime;
  doc["device_id"]    = "ESP32-BAI2-B23DCAT076";

  // 6. Publish lên MQTT Broker
  Serial.println("[MQTT] Kiem tra dong ho truoc khi publish telemetry...");
  // Timestamp UTC immediately before serialization/publish; no Serial I/O in between.
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  unsigned long syncAgeMs = millis() - lastNtpSyncMs;
  bool clockReady = ntpSynced && tv.tv_sec > 1704067200 && syncAgeMs <= 20000;
  if (!clockReady) {
    Serial.printf("[CLOCK] SKIP TELEMETRY: NTP chua dong bo hoac qua han "
                  "(ever_synced=%s, sync_age_ms=%lu). Cho NTP dong bo lai.\n",
                  ntpSynced ? "true" : "false", ntpSynced ? syncAgeMs : 0UL);
    // loop() still services Wi-Fi/MQTT; SNTP continues in the background.
    // Do not advance sequence or publish an invalid-timestamp telemetry packet.
    return;
  }
  doc["sequence"] = ++sequenceNo;
  doc["ntp_synced"] = true;
  doc["ntp_sync_age_ms"] = syncAgeMs;
  doc["ntp_sync_count"] = ntpSyncCount;
  doc["sent_at_ms"] = (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
  char payload[512];
  serializeJson(doc, payload, sizeof(payload));
  bool success = mqttClient.publish(MQTT_TOPIC_TELEMETRY, payload);
  Serial.printf("[PAYLOAD JSON] %s\n", payload);

  if (success) {
    Serial.println("[KET QUA] -> GUI THANH CONG (SUCCESS)! [OK]");
    digitalWrite(LED_PIN, HIGH);
    delay(100);
    digitalWrite(LED_PIN, LOW);
  } else {
    Serial.println("[KET QUA] -> GUI THAT BAI (FAILED)!");
  }
}
