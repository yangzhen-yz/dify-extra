import os
import pika
import json
import requests
import logging
import time
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Authenticator:
    def __init__(self):
        self.access_token = None
        self.expiry_time = None
        self.login_url = os.environ.get('LOGIN_URL')
        self.email = os.environ.get('EMAIL')
        self.password = os.environ.get('PASSWORD')
    
    def get_token(self):
        if not self.access_token or datetime.now() >= self.expiry_time:
            self.refresh_token()
        return self.access_token
    
    def refresh_token(self):
        payload = {
            "email": self.email,
            "password": self.password,
            "language": "zh-Hans",
            "remember_me": True
        }
        
        try:
            response = requests.post(self.login_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if data.get("result") == "success":
                self.access_token = data["data"]["access_token"]
	       # 正确计算55分钟后过期（替换原来的replace方式）
                self.expiry_time = datetime.now() + timedelta(minutes=55)
                logger.info(f"成功获取access_token，有效期至: {self.expiry_time}")
            else:
                logger.error(f"登录失败: {data}")
                
        except Exception as e:
            logger.error(f"获取token时发生错误: {str(e)}")
            raise

class MessageHandler:
    def __init__(self):
        self.auth = Authenticator()
        self.api_key_url = os.environ.get('API_KEY_URL')
        
    def process_message(self, ch, method, properties, body):
        try:
            app_data = json.loads(body)
            app_id = app_data["id"]
            logger.info(f"收到新应用创建消息: {app_id}")
            
            # 创建API Key
            self.create_api_key(app_id)
            
            # 确认消息处理完成
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError as e:
            logger.error(f"消息解析错误: {str(e)}")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # 无效消息直接确认
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            # 可以选择nack并让消息重新入队重试
            

    def create_api_key(self, app_id):
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        payload = {}  # 根据实际API要求设置参数
        
        logger.info(f"准备为应用 {app_id} 创建API Key")
        logger.debug(f"使用token: {token[:20]}...{token[-20:]}")
        
        try:
            url = self.api_key_url.format(app_id=app_id)
            logger.info(f"调用API: {url}")
            
            response = requests.post(url, headers=headers, json=payload)
            
            # 检查HTTP状态码
            if response.status_code == 201:
                data = response.json()
                # 根据实际返回结构判断成功
                if "token" in data:  # 假设成功返回中包含token字段
                    logger.info(f"API Key创建成功，应用ID: {app_id}, API Key: {data['token'][:10]}...")
                    return data
                else:
                    logger.warning(f"API返回格式异常，不包含token字段: {data}")
                    return data
            else:
                # 非201状态码视为失败
                response.raise_for_status()
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"API调用HTTP错误: {str(e)}", exc_info=True)
            logger.error(f"响应内容: {response.text}")
            raise
        except Exception as e:
            logger.error(f"创建API Key异常: {str(e)}", exc_info=True)
            raise

class ConsumerService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.handler = MessageHandler()
        self.rabbitmq_url = os.environ.get('RABBITMQ_URL')
        self.queue_name = os.environ.get('QUEUE_NAME')
        
    def connect(self):
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.URLParameters(self.rabbitmq_url)
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                self.channel.basic_qos(prefetch_count=1)
                
                self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=self.handler.process_message
                )
                
                logger.info("消息消费者已启动，等待消息...")
                self.channel.start_consuming()
                return
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"RabbitMQ连接尝试 {attempt+1}/{max_retries} 失败: {str(e)}")
                time.sleep(retry_delay)
        
        raise Exception("无法连接到RabbitMQ，已达到最大重试次数")
            
    def stop(self):
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("消息消费者已停止")

# 主程序
def main():
    service = ConsumerService()
    try:
        service.connect()
    except KeyboardInterrupt:
        service.stop()
        logger.info("服务已手动停止")

if __name__ == "__main__":
    main()
