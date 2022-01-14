# Mid-Term Project: Machine Learning Predictor of Flight Delays
This repository cointains all the files needed to review and recreate the work that was done in the creation and training of a flight delays predictor. Work on this assignment was completed by:
- Kyle McElheran    ([Mkyleran](https://github.com/Mkyleran))
- Robert Pal        ([rj-pal](https://github.com/rj-pal))
- Doron Feldman     ([dafelds](https://github.com/dafelds))


### Data

This model was trained using data colelcted from the air travel industry and distributed by Lighthouse Labs as part of the assignment. Four separate tables have been used

1. **flights**: The departure and arrival information about flights in US in years 2018 and 2019.
2. **fuel_comsumption**: The fuel comsumption of different airlines from years 2015-2019 aggregated per month.
3. **passengers**: The passenger totals on different routes from years 2015-2019 aggregated per month.
5. **flights_test**: The departure and arrival information about flights in US in January 2020. This table will be used for evaluation. For submission, we are required to predict delays on flights from first 7 days of 2020 (1st of January - 7th of January). We can find sample submission in file _sample_submission.csv_

The data are stored in the Postgres database. You can see the information about the hostname and credentials [in Compass](https://data.compass.lighthouselabs.ca/23284197-327b-4c82-84fa-f220a40a7d1a). 


### Folders Structure

##### Data
- This is where we collected and stored the various tables of data that were used in creation and training of our ML model. It includes statistical anlyses of subsets therein.

##### src
- This is where the programming files used in the brainstorming and generation of the model are kept. It includes a modules subfolder that host some of the custom functions that were scripted to help with development.

##### models
- This holds the pickle values of the different ML models that were tested.