*** Settings ***
Resource    ../Data/InputData.robot
Library    SeleniumLibrary

*** Variables ***
#BatDongSan.com
${BtnRead}    css=a[id='readBtn']
${DivText}    css=div[class='box-chap box-chap-3912400']

#Menu search
${Textbox_search}    css=input[id='search-suggestion']

${Real_estate_select_box}    css=div[id='divCategoryRe']
${Apartment_item}    css=li[vl='324']
${Home_item}    css=li[vl='362']
${Nha_rieng}    css=li[vl='41']
${Nha_biet_thu}     css=li[vl='325']
${Nha_mat_pho}    css=li[vl='163']
${Land_type_item}    css=li[vl='361']
${Dat_nen}    css=li[vl='40']
${Ban_dat}    css=li[vl='283']
${Resort_item}    css=li[vl='44']
${Stock_item}    css=li[vl='45']
${Other_real_estate_item}    css=li[vl='48']

${City_select_box}     css=div[class='select-control city-control']
${TxtKhuVuc}    css=input[id='select-context-content']
${Area}    xpath=//li[@class='advance-options adv-arrow' and text()='']

${Price_select_box}    css=div[class='select-control price-control']
#dientich
${Area_box}    css=div[class='select-control area-control']
${Area_min}    css=input[id='txtAreaMinValue']
${Area_max}    css=input[id='txtAreaMaxValue']

${Project_select_box}    css=div[class='select-control project-control']
${Project_text_box}    css=input[placeholder='Tìm Dự án']

${Filter_select_box}    css=div[class='select-control filter-control']

${BtnSearch}    css=input[id='btnSearch']
*** Variables ***
${count_num}    css=span[id='count-number']
*** Keywords ***


Go to Url
    [Arguments]     ${url}     ${pi}
    #${url} =    get element attribute    css=a[pid='${pi}']    href
    go to    ${url}/p${pi}
Click footer page
    [Arguments]    ${i}
    run keyword and continue on failure    scroll element into view    css=a[pid='${i}']
    run keyword and continue on failure    click link    css=a[pid='${i}']

Go to detail page
    [Arguments]    ${iterator}
    run keyword and continue on failure    wait until element is visible    css=div[prid='${iterator}']    10s
    run keyword and continue on failure    click element    css=div[prid='${iterator}']
Go to detail page using href
    [Arguments]    ${href_attribute}
    run keyword and continue on failure    go to    ${href_attribute}
Get text from page story
    click element    ${BtnRead}
    ${TextDoc} =    get text    ${DivText}
    [Return]    ${TextDoc}
Select erea
    [Arguments]    ${area}
    wait until element is enabled    ${City_select_box}    15s
    click element    ${City_select_box}
    input text    ${TxtKhuVuc}  ${area}
    click element     xpath=//li[@class='advance-options adv-arrow' and text()='${area}']
    click element    ${BtnSearch}
    ${countNumber} =    get text    ${count_num}
    sleep    5s
    [Return]    ${countNumber}

Get count number2
    click element    ${BtnSearch}
    ${countNumber} =    get text    ${count_num}
    [Return]    ${countNumber}


