CREATE TABLE restaurants(
   Establishment                     CHAR(76) NOT NULL
  ,Permit_Holder                     CHAR(108)
  ,Telephone                         CHAR(13)
  ,Establishment_Permit_             INTEGER  NOT NULL
  ,Establishment_Address_Street_     CHAR(97)
  ,Establishment_Address_Street_Name CHAR(32)
  ,Establishment_Address_Unit_       CHAR(23)
  ,Establishment_Address_City        CHAR(18)
  ,Establishment_Address_State       CHAR(6)
  ,Establishment_Address_Zip_Code    INTEGER 
  ,Mailing_Address_Street_           CHAR(98) NOT NULL
  ,Mailing_Address_Street_Name       CHAR(28)
  ,Mailing_Address_Unit_             CHAR(26)
  ,Mailing_Address_City              CHAR(19)
  ,Mailing_Address_State             CHAR(14) NOT NULL
  ,Mailing_Address_Zip_Code          INTEGER 
  ,Mailing_Care_Of                   CHAR(44)
  ,PO_BOX                            CHAR(6)
  ,Facility_Permit_                  INTEGER  NOT NULL
  ,Business_Status                   CHAR(4)
  ,Facility_Type                     CHAR(21)
  ,Risk_Category                     CHAR(11)
  ,1st_Inspection                    CHAR(22)
  ,TMKZone                           INTEGER 
  ,TMKSection                        INTEGER 
  ,TMKPlat                           INTEGER 
  ,TMKParcel                         INTEGER 
  ,Permit_Expire_Date                CHAR(23)
);