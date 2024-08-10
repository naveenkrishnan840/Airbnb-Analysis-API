# Airbnb Data Visualization
  ## Based on Airbnb data to create user interface single application and data visulization chart and separate power Bi visulization in power bi desktop 
  ## Domain: Travel Industry, Property Management and Tourism 

## Tech Stack:
  - Frontend:
      - JavaScript
      - Reactjs
      - Material UI
      - Tailwindcss
      - React leafet Map
      - Material UI charts
  - Backend:
      - Python
      - FastApi
      - Maongodb
      - PyMongoArrow
      - pandas
      - uviocorn
      - dotenv

In This project we have Reactjs as frontend, fastapi as backend used to build User interface single application responsively.

## Run the frontend server go to airbnb-analysis-UI folder and go through below command
  `npm install`
  `npm start`

## Run the fastApi server go to airbnb-analysis-API folder and go through below command
  `pip install poetry`
  `poetery install`
  `poetry run start`
  
## Project Structure for backend
- main.py
    - It is used to run fast api to top of uvicorn server. each function have separate uri for maintain request.
- database connection.py
    - It is used to connect the mongodb connection.
- prepare_data_for_powerbi.py
    - It is used to prepare the csv data from airbnb-analysis data. whatever they need based on, I have prepared data as csv file.
- pyproject.py
    - this file is used install all required package in the form of peotry environment

## Project Structure for frontend
  - Frontend is separate github repo
  - In repo have maintain how to run the react application.
  
