SELECT *
INTO PROPTRANS FROM utpropertyinfo;

WITH DATA AS
  (
    SELECT PropertySequenceNumber, TransId, MAX(LoadID) AS LoadID, Count(*)
    FROM PROPTRANS
    GROUP BY PropertySequenceNumber, TransId
    HAVING COUNT(*) > 1
  )
DELETE FROM PROPTRANS 
USING DATA 
WHERE DATA.TransId= PROPTRANS.TransId
AND DATA.PropertySequenceNumber = PROPTRANS.PropertySequenceNumber
AND DATA.LoadID > PROPTRANS.LoadID;

SELECT TransId, LoadID,
                         RecordingDate, DocumentDate, SignatureDate, EffectiveDate,
                         SalesPriceAmount, LoanAmount,
                         SalesPriceAmountStndCode, LoanAmountStndCode,
                         # These remaining variables may be helpful to, although possibly not sufficient for, data cleaning. See documentation for all possible variables.
                         DataClassStndCode, DocumentTypeStndCode,
                         PartialInterestTransferStndCode, IntraFamilyTransferFlag, TransferTaxExemptFlag,
                         PropertyUseStndCode, AssessmentLandUseStndCode,
                         OccupancyStatusStndCode
INTO TRANS FROM utmain
WHERE DataClassStndCode IN ('D', 'H', 'F', 'M'); 


WITH DUP AS
	(
		SELECT TransId, MAX(LoadID) AS LoadID, COUNT(*)
		FROM TRANS
		GROUP BY TransId
		HAVING COUNT(*) > 1
	)
DELETE FROM TRANS 
USING DUP
WHERE DUP.TransId = TRANS.TransId
AND DUP.LoadID > TRANS.LoadID;



SELECT *
INTO HEDONCIS
FROM PROPTRANS
INNER JOIN TRANS USING(TransId)





