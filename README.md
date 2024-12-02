# Open FAIR

## Usage

First, install the required packages from the `requirements.txt` file:

    pip install -r requirements.txt

Next, use `docker-compose up` to start the Kafka cluster and the consumer/producer:

    docker-compose up -d

For using wandb logging, you should have a file called .env containing your wandb api key under the WANDB_API_KEY voice:

```env
# .env file content:

WANDB_API_KEY=your_wandb_api_key_here

```
