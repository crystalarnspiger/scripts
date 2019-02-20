DO $$
   DECLARE myxml xml;
BEGIN

myxml := XMLPARSE(DOCUMENT convert_from(pg_read_binary_file('RG_Points_xml.xml'), 'UTF8'));

DROP TABLE IF EXISTS rg.xml_table;
CREATE TABLE rg.xml_table AS

SELECT
     (xpath('//fileIdentifier/text()', x))[1]::text AS id
    ,(xpath('//contact/CI_ResponsibleParty/individualName/text()', x))[1]::text AS Name
    -- ,(xpath('//RFC/text()', x))[1]::text AS RFC
    -- ,(xpath('//Text/text()', x))[1]::text AS Text
    -- ,(xpath('//Desc/text()', x))[1]::text AS Desc
FROM unnest(xpath('//record', myxml)) x
;

END$$;


SELECT * FROM rg.xml_table;
