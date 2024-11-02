import data_cleaning
import raster 
import lomb_scargle
import sys
import pandas as pd
import os 

def main():
    #Process user input: filename for processing, whether the DF should be binarized, and what to do with LS data
    filename = str(sys.argv[1])
    binarized = bool(sys.argv[2])
    result_type_ls = str(sys.argv[3])
    logbook = str(sys.argv[4])
    provided_group_name = str(sys.argv[5])

    if logbook == "None":
        logbook = None

    if provided_group_name == "None":
        provided_group_name = None

    logbook_df, logbook_subjects, logbook_groups, naming_group = data_cleaning.logbook_generator(logbook, provided_group_name)

    #Create dataframe
    df, logbook_subjects, logbook_df, flawed_data, num_deleted_rows, start_date, end_date = data_cleaning.data_organizer(filename, logbook_subjects, logbook_df)

    if num_deleted_rows > 0:
        print("Number of deleted rows due to flaws in data:", num_deleted_rows)

        print(flawed_data)

    """
    #Get information from the name of the csv file
    group_name, light_condition, start_date, end_date, path, two_lights = data_cleaning.info_from_naming_pattern(filename)
    """

    #Using the "Light" column, determine the LD, DD, and LL days of the experiment
    condition_days, condition_keys, light_condition = data_cleaning.light_code(df)

    end_date_str = f"{end_date.month:02d}{end_date.day:02d}"

    path = naming_group + "_" + light_condition + "_" + end_date_str

        # The path combines the group name, light condition, and
        # end date of the experiment
        # It is later used to create folders to store graphs in


    #Resample data to pass into raster plot
    df_processed = data_cleaning.resample_df_six_mins(df, logbook_subjects, binarize = binarized)

    df.set_index('Time', inplace=True)
    flawed_data.set_index('Time', inplace=True)

    print(flawed_data)

    raster_path = path + "_raster_plots"
    LS_path = "LS_" + path

    if not os.path.exists(LS_path) and result_type_ls != 'value':
        os.makedirs(LS_path)

    if not os.path.exists(raster_path):
        os.makedirs(raster_path)

    #display(df_processed)
    filepath = f"{naming_group}_{end_date_str}_LS_info.txt"

    """# Ensure 'Time' column is in datetime format
    if not np.issubdtype(df['Time'].dtype, np.datetime64):
        df_processed['Time'] = pd.to_datetime(df['Time'])
    
    # Set 'Time' as the index
    df.set_index('Time', inplace=True)"""

    """
    with open(filepath, "w") as info_file:
        print(spiders)
        for i in spiders:
            spider_column = f"Sp{i:02d}"  # Create the correct column name
            print(spider_column)
            print(spider_column[-2:])
            raster.raster_plot(df_processed, spider_column, group_name, end_date, raster_path, condition_days, average_raster=True)
            for light_con in condition_keys:
                period, fap = lomb_scargle.period_LS(df, spider_column, light_con, condition_days, group_name, info_file, LS_path, end_date, result_type=result_type_ls)
    """

    with open(filepath, "w") as info_file:
        print(logbook_subjects)
        for subject in logbook_subjects:
            print()
            print()
            print(subject)
            print()
            raster.raster_plot(df_processed, subject, naming_group, end_date_str, raster_path, condition_days, flawed_data, average_raster=True)
            for light_con in condition_keys:
                period, fap = lomb_scargle.period_LS(df, subject, light_con, condition_days, info_file, LS_path, end_date_str, result_type=result_type_ls)
    


    raster.combined_raster(raster_path, naming_group, end_date_str, figsize=(15, 10))


if __name__ == "__main__":
    main()