*** Settings ***
Library    ../CustomLibs/Csv.py
Library    OperatingSystem
Library    DateTime
*** Keywords ***
Get csv data
    [Arguments]    ${file_path}
    ${Data} =    read csv file    ${file_path}
    [Return]   ${Data}

Generate file path
    [Arguments]    ${website}
    ${now}    Get Time    epoch
    ${CurrentDate}    Get Current Date    result_format=%d-%m-%Y
    Log    ${CurrentDate}
    ${file_path}    Join Path    ${OUTPUT DIR}    ${website}_${CurrentDate}_${now}.csv
    [Return]    ${file_path}

Write data to file
    [Arguments]    ${file_path}    ${text1}    ${text2}    ${text3}    ${text4}    ${text5}
    write_csv_file_2   ${file_path}    ${text1}    ${text2}    ${text3}    ${text4}    ${text5}