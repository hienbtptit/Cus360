*** Settings ***
Documentation    Crawler data from BatDongSan.com
Resource    ../Resource/Common.robot
Resource    ../Resource/DataManager.robot
Resource    ../Data/InputData.robot
Resource    ../Resource/PO/LandingPage.robot
Resource    ../Resource/PO/DetailPost.robot


Test Setup    Common.Begin web test
Test Teardown    Common.End web test

*** Test Cases ***

Crawler bds the first time
    ${file_name} =    DataManager.Generate file path    BDS
    log    ${file_name}
    run keyword and continue on failure    Navigate to    ${BĐS_URL}
    sleep    1s
    Select erea    ${SEARCH_AREA}
    ${url} =    get element attribute    css=a[pid='1']    href
    FOR    ${i}    IN RANGE    1    10
       Go to Url    ${url}    ${i}

       ${list_element} =    run keyword and continue on failure    Create List
       ${elements}    run keyword and continue on failure    Get all element has same attribute    css=a[class='wrap-plink']
        FOR    ${element}    IN    @{elements}
            Log    ${element}
            Append To List   ${list_element}   ${element.get_attribute('href')}
        END

        FOR    ${iterator}    IN    @{list_element}
            Go to detail page using href  ${iterator}
            Get post infor    ${file_name}
        END
    END
