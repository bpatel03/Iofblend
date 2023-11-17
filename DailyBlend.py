import pandas as pd
from pulp import *
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os

file_path = os.path.join(os.path.dirname(__file__),'iofblending-82f619a347d1.json')


# Authenticate with Google Sheets

#GDOCS_OAUTH_JSON       = 'iofblending-82f619a347d1.json'
GDOCS_OAUTH_JSON       = file_path

# Google Docs spreadsheet name.
GDOCS_SPREADSHEET_NAME = 'BlendRecomendation'
    
# How long to wait (in seconds) between measurements.
FREQUENCY_SECONDS      = 10
    
    
def login_open_sheet(oauth_key_file, spreadsheet):
    """Connect to Google Docs spreadsheet and return the first worksheet."""
    try:
        scope =  ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(oauth_key_file, scope)
        gc = gspread.authorize(credentials)
        worksheet = gc.open(spreadsheet).sheet1
        return worksheet
    except Exception as ex:
        print('Unable to login and get spreadsheet.  Check OAuth credentials, spreadsheet name, and make sure spreadsheet is shared to the client_email address in the OAuth .json file!')
        print('Google sheet login failed with error:', ex)
        sys.exit(1)

def blend_Optimization():
    sheet_id = "1nuNtz1jwXyd56AD5R-RQT2KRQXP-ConsV49HIZniSzo"
    r = "https://docs.google.com/spreadsheets/d/{}/export?format=csv".format(sheet_id)
    df = pd.read_csv(r)
    df1 = df
    df1 = df1.dropna()

    sheet_id01 = "1gW3hAaSQ-r_GQNm0rs377-yXvsSwon7WM9Hr4BjM-FE"
    r1 = "https://docs.google.com/spreadsheets/d/{}/export?format=csv".format(sheet_id01)
    dfT = pd.read_csv(r1)

    
    days = dfT['Day'].tolist()
    Act_Blend_cost = dfT['Total_Cost'].tolist()
    Act_FE = dfT['FE'].tolist()
    Act_SI = dfT['SI'].tolist()
    Act_AL = dfT['AL'].tolist()
    Act_LOI = dfT['LOI'].tolist()

    # Convert DataFrame columns to separate dictionaries
    ores = df1['Mines'].tolist()
    prices = df1.set_index('Mines')['Prices'].to_dict()
    Initial_Stocks = df1.set_index('Mines')['Stock'].to_dict()
    fe_percentages = df1.set_index('Mines')['Fe%'].to_dict()
    al_percentages = df1.set_index('Mines')['AL%'].to_dict()
    si_percentages = df1.set_index('Mines')['SI%'].to_dict()
    loi_percentages = df1.set_index('Mines')['LOI%'].to_dict()

    i=0
    res_rows = []

    for day in range(1, len(days)+1):
        FE = Act_FE[i]
        SI = Act_SI[i]
        AL = Act_AL[i]
        LOI = Act_LOI[i]
        
        # Create the linear programming problem
        prob = LpProblem("Ore_Blending", LpMinimize)
        
        # Define the variables
        ore_vars = LpVariable.dicts("Ore", ores, lowBound=0, upBound=0.5, cat='Continuous')

        # Add a constraint to ensure ore_vars[ore] is non-negative for each ore
        for ore in ores:
            prob += ore_vars[ore] >= 0

        
        # Define the objective function (minimize the total price)
        prob += lpSum([prices[ore] * ore_vars[ore] for ore in ores])
         # Define the constraints
        prob += lpSum([ore_vars[ore] for ore in ores]) == 1.0  # Constraint: Total percentage is 100%
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) >= FE  # Constraint: Fe% is at least 63
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) <= FE+0.3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) <= AL  # Constraint: Al% is less than or equal to 3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) >= AL-0.3
        prob += lpSum([loi_percentages[ore] * ore_vars[ore] for ore in ores]) <= LOI
        
        # Solve the problem
        prob.solve()
        
        # Calculate the blended Fe%, AL%, SI%, and LOI%
        blended_fe = sum([value(ore_vars[ore]) * fe_percentages[ore] for ore in ores])
        blended_al = sum([value(ore_vars[ore]) * al_percentages[ore] for ore in ores])
        blended_si = sum([value(ore_vars[ore]) * si_percentages[ore] for ore in ores])
        blended_loi = sum([value(ore_vars[ore]) * loi_percentages[ore] for ore in ores])

        # Check the status of the solution
        if LpStatus[prob.status] != "Optimal":
            print(f"For day={day}, no optimal solution found.")
        else:
            
            #break
             res_rows.append([day,round(FE,2),round(SI,2),round(AL,2),round(LOI,2),round(Act_Blend_cost[i],0), round(value(prob.objective)),round(Act_Blend_cost[i]-(round(value(prob.objective))),0)] +  [round(blended_fe, 2), round(blended_al, 2), round(blended_si, 2), round(blended_loi, 2)]+[round(value(ore_vars[ore]) * 100, 2) for ore in ores] )

        # Print the results
        i=i+1

    # Create the DataFrame after the loop
    res_df = pd.DataFrame(res_rows, columns=['days','Act_FE','Act_SI','Act_AL','Act_LOI','Act_Blend_cost','Blend_cost','P/L'] +  ['blended_fe', 'blended_al', 'blended_si', 'blended_loi']+['Cons_' + ore + '%' for ore in ores] )
    
    file_path = os.path.join(os.path.dirname(__file__),'iofblending-82f619a347d1.json')
    #credentials_file = 'iofblending-82f619a347d1.json'
    credentials_file = file_path
    
    # Authenticate using the service account credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    gc = gspread.authorize(credentials)

    # Open the Google Sheet by its title
    sheet_title = "BlendRecomendation"  # Change this to the title of your Google Sheet
    sh = gc.open(sheet_title)

    # Create a new worksheet or use an existing one
    
    today = datetime.now()
    date_and_time = today.strftime("%d-%b-%Y %H:%M")
    worksheet_title = "Results" + "_" + date_and_time
    
    
    try:
        worksheet = sh.add_worksheet(title=worksheet_title, rows=1, cols=1)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.get_worksheet_by_title(worksheet_title)

    # Write the DataFrame to the Google Sheet
    set_with_dataframe(worksheet, res_df)

    #return res_df

blend_Optimization()

