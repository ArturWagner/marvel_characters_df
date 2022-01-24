# Marvel Characters DF
This repository intends to create a Dataframe with data from [characters](https://developer.marvel.com/docs#!) request

## Usage
You can run this project locally or using Docker.


### Locally
The following steps are to run locally:

1. Install needed Python libs.

```python
  pip install -r requirements.txt
```

2. Set API Keys as enviroment variables.
```bash
  export PUBLIC_KEY=S0M3pUBl1CK3Y
  export PRIVATE_KEY=S0M3pR1V3T3k3y
```

3. Then run the main script.

```python
  python marvel_characters_df.py
```

Note: This running step will only show logs to export the dataframe to a local csv uncommnet line 252


### Docker
The following steps are to run through docker:

1. Build docker image.

```bash
  docker build -t marvel_characters_df .
```

2. Set API Keys as enviroment variables.
```bash
  export PUBLIC_KEY=S0M3pUBl1CK3Y
  export PRIVATE_KEY=S0M3pR1V3T3k3y
```

3. Then run the docker container.

```bash
  docker run -e PUBLIC_KEY=$PUBLIC_KEY -e PRIVATE_KEY=$PRIVATE_KEY  marvel_characters_df
```

Note: This running step will only show logs to export the dataframe to a local csv uncommnet line 252 and build the docker image again and make sure to map a docker volume.

## Remarks:
The objective of this code was to reach a simple dataframe but the request code was built thinking about a possible evolution to a singer-tap or an airbyte connector.
Apart from achiving the technical goal, one of the purposes of this code is to show some good software development pratices in python.
