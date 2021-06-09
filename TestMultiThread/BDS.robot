*** Settings ***
Documentation    Crawler data from BatDongSan.com
Resource    ../Resource/Common.robot
Resource    ../Resource/DataManager.robot
Resource    ../Data/InputData.robot
Resource    ../Resource/PO/LandingPage.robot
Resource    ../Resource/PO/DetailPost.robot


Test Setup    Common.Begin web test
Test Teardown    Common.End web test
*** Variables ***
${page_locator} =     css=a[pid='1']
${post_locator} =    css=a[class='wrap-plink']
*** Test Cases ***

Crawler bds the first time
    [Arguments]    ${START}    ${END}
    ${file_name} =    DataManager.Generate file path    BDS
    log    ${file_name}
    Write data to file   ${file_name}  prid    title   des   phone    date
    run keyword and continue on failure    Navigate to    ${BĐS_URL}
    sleep    1s
    Select erea    ${SEARCH_AREA}
    ${url} =    get element attribute    ${page_locator}   href
    FOR    ${i}    IN RANGE    ${START}   ${END}
       Go to Url    ${url}    ${i}
       ${list_element} =    run keyword and continue on failure    Create List
       ${elements}    run keyword and continue on failure    Get all element has same attribute    ${post_locator}
        FOR    ${element}    IN    @{elements}
            Log    ${element}
            Append To List   ${list_element}   ${element.get_attribute('href')}
        END

        FOR    ${iterator}    IN    @{list_element}
            Go to detail page using href  ${iterator}
            Get post infor    ${file_name}
        END
    END
