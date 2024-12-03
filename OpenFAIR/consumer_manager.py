import threading


class ConsumerManager:

    def __init__(self, consumers, CONSUMER_COMMAND="python consume.py"):
        self.consumers = consumers
        self.threads = {}
        self.consumer_command = CONSUMER_COMMAND


    def start_producer(self, consumer_name, consumer_container):
        def run_consumer():
            return_tuple = consumer_container.exec_run(
                self.consumer_command, 
                stream=True, 
                tty=True, 
                stdin=True
            )
            for line in return_tuple[1]:
                print(f"{consumer_name}: {line.decode().strip()}")

        thread = threading.Thread(target=run_consumer, name=consumer_name)
        thread.start()
        self.threads[consumer_name] = thread
        print(f"Started consumer from {consumer_name}")


    def stop_consumer(self, consumer_name):
        container = self.consumers[consumer_name]
        try:
            # Try to find and kill the process
            pid_result = container.exec_run(f"pgrep -f '{self.consumer_command}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                container.exec_run(f"kill -SIGINT {pid}")
                print(f"Stopped consumer from {consumer_name}")
            else:
                print(f"No running process found for {consumer_name}")
        except Exception as e:
            print(f"Error stopping {consumer_name}: {e}")


    def stop_all_consumers(self):
        for producer_name in self.consumers:
            self.stop_consumer(producer_name)