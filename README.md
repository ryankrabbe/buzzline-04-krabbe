# soccer-project-krabbe

## Overview of my project

The goal of my project is to analyze random soccer data from 50 English Premier League matches. I am aiming to build a database to store my data to identify different match trends as well as creating dynamic real-time visualization to identify performance trends over time.

I created a consumer named project_consumer_krabbe to visualize real time streaming data. My consumer file pulls messages from the Kafka Topic I created, which is random soccer matches and then creates a dynamic visualization to identify performance trends over time. My consumer that I created reads the data sent from the producer file named project_producer_case which sends the data from a json file that I created named soccer_matches.json

## Visualization
In it's most simple form the bar chart provides a real time view of different match statistics for different teams based on the game. I used matplotlib for my visualizations and implemented numpy for trend analysis.
- X-Axis - Teams
- Y-Axis - Goals (green), Shots (blue), Fouls (red)

To take my visualization to the next level I added trend analysis using to identify performance trends over time. From the trend analysis different observations can be made including:
- Team Performance Over Time
- Momentum Indicators
- Discipline Trends (based on fouls committed)

Below is a screenshot of my figure offering a real time view of different match statistics including trend analysis.
![Real Time Visualization](Screenshot_2025-02-18_143408.png)

## Storing Data in SQLite
My soccer project takes the live streaming data from Kafka and stores it in a SQLite database that I created. I created two different tables: matches & team_stats.
- Matches Table - The matches data takes the raw data that is being streamed and stores it inside the table
- team_stats Table - This table stores metrics such as total goals, fouls and shots over multiple games

Both of these tables help store data in a simple format and make it easier to track and analyze different match trends.

We can analyze and visualize different types of streaming data as the information arrives.

### Follow the steps below to run the Producer & Custom Consumer on your own!

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create your .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.

## Task 4. Start Zookeeper and Kafka (2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 5. Start a Basic (File-based, not Kafka) Streaming Application

This will take two terminals:

1. One to run the producer which writes to a file in the data folder. 
2. Another to run the consumer which reads from the dynamically updated file. 

### Producer Terminal

Start the producer to generate the messages. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.project_producer_case
```

### Consumer Terminal

Start the associated consumer that will process and visualize the messages. 

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.project_consumer_krabbe
```

