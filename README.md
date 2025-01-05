# Open FARI

This is the official implementation of the paper:

## Open-FARI: An Open-source testbed for Federated Anomaly detection in the Railway IoT

from Alessandra Rizzardi, Raffaele Della Corte, Jesus F. Cevallos M., Vittorio Orbinato, Simona De Vivo, Sabrina Sicari, Domenico Cotroneo, and Alberto Coen-Porisini

If you find this code useful, please cite us:

[CITATION]

## Usage

### Clone the repo:

This is a nested repo, so be sure to clone the repo recursively, using the `--recursive` flag with the `git clone` command. Here's the syntax:

```bash
git clone --recursive https://github.com/DIETI-DISTA-IoT/OF
```

This will clone the repository and all its submodules.

Alternatively, if you've already cloned the repository without the `--recursive` flag, you can use the following command to initialize and update the submodules:

```bash
git submodule update --init --recursive
```

This will fetch and checkout the submodules recursively.


### Installation:

You may want to create a virtual environment before installing python libraries:


   ```bash
   python3 -m venv <name-of-the-venv>
   ```

Activate the virtual environment. The command to activate the virtual environment depends on your operating system:

   - On macOS and Linux:

     ```bash
     source <name-of-the-venv>/bin/activate
     ```

   - On Windows:

     ```bash
     <name-of-the-venv>\Scripts\activate
     ```

Once the virtual environment is activated, you can install packages and run Python scripts within the virtual environment without affecting your global Python installation. Install the required packages to launch the container manager from the `requirements.txt` file:

    pip install -r requirements.txt

## Building vehicle images and dashboard:

Use `make all` to build the producer and consumer docker images. 

### Launching:

Use `docker compose up` to start the whole cluster. Otherwise, start launching only the kafka and zookeeper

    docker-compose up -d

**IMPORTANT** For using the wandb logging dashboard, you should have a file called .env in the project's root folder containing your wandb api key under the WANDB_API_KEY voice:

```.env file
# .env file content:

WANDB_API_KEY=your_wandb_api_key_here

```
Run the container manager script:

    python dashboard/app.py

Adjust configurations for this script in the `config/default.yaml` or create an ovverride `*.yaml` configuration on the `config/override` directory that you can use to override a subset of params. To launch an override conf, use:

    python dashboard/app.py override=my_conf_filename

Comand-line args can be sent also using the hydra syntax (i.e. no hyphens) and created appending `+` 

    python dashboard/app.py +foo=bar

