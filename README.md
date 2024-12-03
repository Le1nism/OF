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
Run the container manager script:

    python container_manager_server.py

Adjust configurations for this script in the `config/default.yaml` or create an ovverride `*.yaml` configuration on the `config/override` directory that you can use to override a subset of params. To launch an override conf, use:

    python container_manager_server.py override=my_conf_filename

Comand-line args can be sent also using the hydra syntax (i.e. no hyphens) and created appending `+` 

    python container_manager_server.py container_manager_port=3012 +foo=bar

