*** Settings ***
Library    SeleniumLibrary
Library    Collections
Library    String
Library    Csv
Resource    ../Data/InputData.robot
Resource    ../DataManager.robot

*** Variables ***
${product_id} =    css=div[id='product-detail-web']
${product_title} =    css=h1[class='pr-title tile-product']
${product_des} =    css=div[class='des-product']
${product_phone} =    css=span[class='phoneEvent']
${post_date} =    css=span[class='sp3']

*** Keywords ***
Get post infor
    [Arguments]    ${file_path}
    ${list}    create list
    run keyword and continue on failure    wait until element is visible  ${product_title}    10s
    ${prid} =    run keyword and continue on failure    get element attribute    ${product_id}    prid
    ${title} =    run keyword and continue on failure    Get text    ${product_title}
    ${des} =    run keyword and continue on failure    Get Text    ${product_des}
    ${phone_element} =    run keyword and continue on failure    get webelement   ${product_phone}
    ${phone}   run keyword and continue on failure    set variable    ${phone_element.get_attribute('raw')}
    ${date}    run keyword and continue on failure    Get text    css=#product-detail-web > div.detail-product > div.product-config.pad-16 > ul > li:nth-child(1) > span.sp3
    Write data to file    ${file_path}  ${prid}    ${title}    ${des}   ${phone}    ${date}