# Open FAIR

## Usage

First, install the required packages from the `requirements.txt` file:

    pip install -r requirements.txt

(Optional) Pre build custom docker images: 

    make all

If you are using just one consumer and producer, then use `docker compose up` to start the whole cluster. Otherwise, start launching only the kafka and zookeeper

    docker-compose up -d zookeeper kafka dashboard

For using wandb logging, you should have a file called .env containing your wandb api key under the WANDB_API_KEY voice:

```.env file
# .env file content:

WANDB_API_KEY=your_wandb_api_key_here

```
