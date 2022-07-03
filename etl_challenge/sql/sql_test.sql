SELECT createdAt, COUNT(*) AS totMsgs FROM message GROUP BY createdAt ORDER BY totMsgs DESC;
SELECT * FROM appuser AU LEFT JOIN message M ON AU.userId = M.receiverId WHERE M.receiverId IS NULL;
SELECT count(*) AS TotActiveSubscriptions FROM subscription WHERE status = 'Active';
SELECT D.dateMonth, Avg(amount) FROM subscription S JOIN datedim D ON S.createdAt = D.dateId GROUP BY D.dateMonth;