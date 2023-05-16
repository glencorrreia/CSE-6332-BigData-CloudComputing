matrixM = LOAD '$M' USING PigStorage(',') AS (row,column,value);
matrixN = LOAD '$N' USING PigStorage(',') AS (row,column,value);

joinP = JOIN matrixM BY column, matrixN BY row;
multiplyP = FOREACH joinP GENERATE matrixM::row AS row,matrixN::column AS column,(matrixM::value)*(matrixN::value) AS value;
groupP = GROUP multiplyP BY (row, column);
add = FOREACH groupP GENERATE $0 as row, SUM(multiplyP.value) AS value;


STORE add INTO '$O' USING PigStorage(',');
dump add
