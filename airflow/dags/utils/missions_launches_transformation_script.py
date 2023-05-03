import pandas as pd


def transform_spase_mission_file(missions_data):
    missions_data = missions_data.iloc[:, 2:].drop_duplicates().reset_index(drop=True)

    missions_data["Country"] = missions_data["Location"].str.split(", ").str[-1]
    missions_data.loc[(missions_data["Country"] == 'Russia'), "Country"] = "Russian Federation"
    missions_data.loc[(missions_data["Country"] == 'New Mexico'), "Country"] = "USA"
    missions_data.loc[(missions_data["Country"] == 'Yellow Sea'), "Country"] = "China"
    missions_data.loc[(missions_data["Country"] == 'Shahrud Missile Test Site'), "Country"] = "Iran"
    missions_data.loc[(missions_data["Country"] == 'Pacific Missile Range Facility'), "Country"] = "USA"
    missions_data.loc[(missions_data["Country"] == 'Barents Sea'), "Country"] = "Russian Federation"
    missions_data.loc[(missions_data["Country"] == 'Gran Canaria'), "Country"] = "USA"
    missions_data.loc[(missions_data["Country"] == 'Iran'), "Country"] = "Iran, Islamic Republic of"
    missions_data.loc[(missions_data["Country"] == 'South Korea'), "Country"] = "Korea, Republic of"
    missions_data.loc[(missions_data["Country"] == 'North Korea'), "Country"] = "Korea, Democratic People's Republic of"
    missions_data.loc[(missions_data["Country"] == 'Kazakhstan'), "Country"] = "Russian Federation"
    missions_data['Price'] = missions_data['Price'].str.replace(',', '').astype(float)
    missions_data['Date'] = pd.to_datetime(missions_data['Date'])

    missions_data.columns = missions_data.columns.str.lower()

    return missions_data
