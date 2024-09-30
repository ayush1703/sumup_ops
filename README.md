Welcome to the Sum Up Operation analytics dbt project!

### Data Model Design:
1. Staging Layer: Loads raw data from CSV files: contact_raw_data
2. Intermediate Layer: Aggregates and transforms data into data marts: Resolution window calculation - 7 days and No limit, Ops performance, Channel cost 
3. Presentation Layer: SQL queries to answer business questions: Channel performance, Agent company performance, Reolution window, Channel costs

Models in Staging layer and intermediate layer are designed to load in Incremental way so that scaling the model with million of rows is possible.

![alt text](<dag_chart.png>)

### Solution implementation Step by step guide to run the project

1. Setting up the environment
    - Create virtual environment: python -m venv dbt-env  
    - Activate virtual environment : source dbt-env/bin/activate
    - Install dbt and duckdb adapter: python -m pip install dbt-core dbt-duckdb 
    - Install duckdb: pip install duckdb: brew install duckdb

2. Setup new DBT project via: dbt init sumup_ops

3. Load CSV files into DuckDB and create staging data models - 1 data model per file

4. Setup config files: dbt_project.yml and profiles.yml

5. Try running the dbt run command and verify if staging data models are loaded correctly.

6. Build tests and create model_properties.yml file to bind test cases and manage documentaion.

7. Create intermediate layer with relevant data models to include dimensions and facts which can be used for analysis tables later. Repeat step 4 - 6 till desired models are ready.

8. Build data models for tasks in analysis - one table per question.

9. Plug duckdb into any visualization tool such as Tableau and use prepared data models to find relevant insights.

10. Generate documentation based on model_properties.yml: dbt docs generate
    
Try running the following commands:
- dbt run
- dbt test

### Assumption
1. Since response time is only available for email channel, it is assumed as total time taken by agent to respond to the touchpoint which can also include delay due to existing backlog.
2. As email touchpoints have highest share and there is only one agent ID assigned to all emails, the agent ID column does not seem to represent unique agents reponsible for answering the touchpoints and hence agent ID column is excluded from all data models before data is validated.
3. The combination of detailed reason and reason along with merchant ID is used to pair the repeat touchpoints together.
4. As time spent on emails is not available, only the volume of tickets is considered for cost estimation. Also, call and email are considered to have equal effort for agent while chat is considered as 1/3 effort.
5. For optimization of resolution window, two scripts are prepared: First script simulates the current scenario of having 7 days as resolution window and check for repeat tickets and time difference between first and last touchpoint. The second script maps all tickets of same reason from a merchant in one window and check how big the window size can be.


### Executive Summary from the analysis
The sales lead and funnel data for customers who purchased Sumup devices in 2023 were analysed to uncover top insights and identify areas for performance improvement. The objective is to understand the best and worst-performing acquisition channels, evaluate the sales funnel, and identify potential bottlenecks impacting lead times. 

Key Insights
1. Touchpoint Overview: SumUp handled 350K customer touchpoints in 2022, with 70.67% of cases resolved. Email was the most used channel (42%), followed by calls (32%) and chats (25%).

2. Performance by Channel: Chat performed best with the highest "perfect case" ratio and lowest handling times, while email had longer response times and higher costs. Call performance lagged in Italy, and email was slowest in France.

3. Agent Company Comparison: BPO2 had the highest throughput and good handling times across channels. BPO1 handled the most cases but performed similarly to BPO2, while SumUp had slower handling times, particularly for emails.

4. Cost Distribution: Email is the costliest channel (55% of the budget), while chat is the most efficient, costing €7 per touchpoint due to the ability to handle multiple chats simultaneously. Calls and emails averaged €20 per touchpoint.

5. Resolution Window Adjustment: A recommended change to the resolution window from 7 days to 5 days for faster resolution, or 10 days to improve case tagging accuracy.

6. Key Insights: Chat is the top-performing channel, while calls and emails need optimization. BPO2 outperformed in agent metrics, and seasonality impacts performance, with a dip from May to July.

7. Recommendations: Focus more on chat, optimize response times in email, reduce email reliance for less urgent queries, and align agent staffing with peak demand times.

8. Performance Management Framework: Implement a quarterly performance review cycle involving KPIs, cost analysis, and cross-functional alignment to ensure continuous improvement and resource optimization.








Based on the insights, we can align with the GTM strategy team to come up with an action plan on improving the sales performance and reducing lead time. 




