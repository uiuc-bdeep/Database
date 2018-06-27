SELECT RowID, ImportParcelID, LoadID,
                       FIPS, State, County,
                       PropertyFullStreetAddress,
                       PropertyHouseNumber, PropertyHouseNumberExt, PropertyStreetPreDirectional, PropertyStreetName, PropertyStreetSuffix, PropertyStreetPostDirectional,
                       PropertyCity, PropertyState, PropertyZip,
                       PropertyBuildingNumber, PropertyAddressUnitDesignator, PropertyAddressUnitNumber,
                       PropertyAddressLatitude, PropertyAddressLongitude, PropertyAddressCensusTractAndBlock,
                       NoOfBuildings,
                       LotSizeAcres, LotSizeSquareFeet,
                       TaxAmount, TaxYear
  	INTO BASE FROM utmain;
WITH DATA AS
	(
		SELECT ImportParcelID, MIN(LoadID) AS LoadID, Count(*)
		FROM BASE
		GROUP BY ImportParcelID
		HAVING COUNT(*) > 1
	)
DELETE FROM BASE 
USING DATA 
WHERE DATA.ImportParcelID = BASE.ImportParcelID
AND DATA.LoadID > BASE.LoadID;

SELECT RowID, NoOfUnits, BuildingOrImprovementNumber,
                       YearBuilt, EffectiveYearBuilt, YearRemodeled,
                       NoOfStories, StoryTypeStndCode, TotalRooms, TotalBedrooms,
                       FullBath, ThreeQuarterBath, HalfBath, QuarterBath,
                       HeatingTypeorSystemStndCode,
                       PropertyLandUseStndCode, WaterStndCode
	INTO BLDG FROM utbuilding
	WHERE PropertyLandUseStndCode IN (		'RR101',  /* SFR*/
                                            'RR999',  /* Inferred SFR*/
                                            'RR104',  /* Townhouse */
                                            'RR105',  /* Cluster Home */
                                            'RR106',  /* Condominium*/
                                            'RR107',  /* Cooperative*/
                                            'RR108',  /* Row House*/
                                            'RR109',  /* Planned Unit Development*/
                                            'RR113',  /* Bungalow*/
                                            'RR116',  /* Patio Home*/
                                            'RR119',  /* Garden Home*/
                                            'RR120'); 


/* WITH BLDG AS (SELECT RowID, NoOfUnits, BuildingOrImprovementNumber,
                       YearBuilt, EffectiveYearBuilt, YearRemodeled,
                       NoOfStories, StoryTypeStndCode, TotalRooms, TotalBedrooms,
                       FullBath, ThreeQuarterBath, HalfBath, QuarterBath,
                       HeatingTypeorSystemStndCode,
                       PropertyLandUseStndCode, WaterStndCode
FROM utbuilding)*/
/*WITH SQFT AS 
	(SELECT * FROM ututbuildingareas
		WHERE BuildingAreaStndCode IN(     'BAL',  
                                           'BAF',  
                                           'BAE', 
                                           'BAG',  
                                           'BAJ',  
                                           'BAT',  
                                           'BLF')
		FROM utbuildingareas)
*/

/*Start the merging of the datasets*/
CREATE TABLE HEDONICS AS (
	WITH ATTR AS (SELECT *
				FROM BASE
				INNER JOIN BLDG USING (RowID)),
	SQFT AS
		(SELECT * FROM utbuildingareas
			WHERE BuildingAreaStndCode IN( 'BAL',  /* Building Area Living*/
                                           'BAF',  /* Building Area Finished*/
                                           'BAE',  /* Effective Building Area*/
                                           'BAG',  /* Gross Building Area*/
                                           'BAJ',  /* Building Area Adjusted*/
                                           'BAT',  /* Building Area Total*/
                                           'BLF')
		)

	SELECT ATTR.*, SQFT.buildingareasequencenumber, SQFT.buildingareastndcode, SQFT.buildingareasqft, SQFT.batchid 
	FROM ATTR
	LEFT JOIN SQFT ON
		ATTR.RowID = SQFT.RowID);




