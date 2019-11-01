------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- ETL Query for ipl.dbo.Delivery_Dim

create table Log_ETL_Delivery_Dim
(
MergeAction Varchar(20),
match_id Int,
inning Int,
over_no Int,
ball Int
);

MERGE ipl.dbo.Delivery_Dim TGT
USING o_ipl.dbo.deliveries SRC 
ON (1=1)
WHEN MATCHED
THEN UPDATE
     SET    TGT.MIBO_ID = CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10)),'-',
							   cast(SRC.Over_No as varchar (10)),'-',
							   cast(SRC.Ball as varchar (10))), 
			TGT.Batsman = SRC.batsman,
            TGT.Non_Striker = SRC.non_striker,
            TGT.Bowler = SRC.bowler,
			TGT.Player_Dismissed = SRC.player_dismissed,
			TGT.Dismissal_Kind = SRC.dismissal_kind,
			TGT.Fielder = SRC.fielder
			

WHEN NOT MATCHED BY TARGET
THEN INSERT (MIBO_ID,
             Batsman, 
			 Non_Striker, 
			 Bowler, 
			 Player_Dismissed,
			 Dismissal_Kind,
			 Fielder)
     VALUES ( CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10)),'-',
							   cast(SRC.Over_No as varchar (10)),'-',
							   cast(SRC.Ball as varchar (10))),
	         SRC.batsman,
			 SRC.non_striker, 
			 SRC.bowler, 
			 SRC.player_dismissed,
			 SRC.dismissal_kind, 
			 SRC.fielder
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.match_id, SRC.inning, SRC.over_no, SRC.ball into Log_ETL_Delivery_Dim;

SELECT MergeAction, count(*)
FROM   Log_ETL_Delivery_Dim
GROUP BY MergeAction;


------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- ETL Query for ipl.dbo.Match_Dim

create table Log_ETL_Match_Dim
(
MergeAction Varchar(20),
match_id Int
);

MERGE ipl.dbo.Match_Dim TGT
USING o_ipl.dbo.matches SRC 
ON (1=1)
WHEN MATCHED
THEN UPDATE
     SET    TGT.Match_ID = SRC.id , 
			TGT.City = SRC.city,
            TGT.Team1 = SRC.team1,
            TGT.Team2 = SRC.team2,
			TGT.Toss_Winner = SRC.toss_winner,
			TGT.Toss_Decision =  SRC.toss_decision, 
			TGT.Result = SRC.result,
            TGT.Winner = SRC.winner,
            TGT.Player_Of_Match = SRC.player_of_match,
			TGT.Venue = SRC.venue,
			TGT.Umpire1 = SRC.umpire1,
			TGT.Umpire2 = SRC.umpire2
			
WHEN NOT MATCHED BY TARGET
THEN INSERT (Match_ID,
             City, 
			 Team1, 
			 Team2, 
			 Toss_Winner,
			 Toss_Decision,
			 Result,
			 Winner,
			 Player_Of_Match,
			 Venue,
			 Umpire1,
			 Umpire2)
     VALUES ( SRC.id,
	         SRC.city,
			 SRC.team1, 
			 SRC.team2, 
			 SRC.toss_winner,
			 SRC.toss_decision, 
			 SRC.result,
			 SRC.winner,
			 SRC.player_of_match, 
			 SRC.venue, 
			 SRC.umpire1,
			 SRC.umpire2
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.id into Log_ETL_Match_Dim;

SELECT MergeAction, count(*)
FROM   Log_ETL_Match_Dim
GROUP BY MergeAction;


------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- ETL Query for ipl.dbo.Date_Dim

create table Log_ETL_Date_Dim
(
MergeAction Varchar(20),
date_id date
);

MERGE ipl.dbo.Date_Dim TGT
USING 
(select distinct convert(date,date_of_match) as date_of_match1 from o_ipl.dbo.matches) SRC
ON (1=1)
WHEN MATCHED
THEN UPDATE
			 SET 
			TGT.Date_ID     =   CAST(CONCAT( DATEPART(d,SRC.date_of_match1),
								DATEPART(m,SRC.date_of_match1),DATEPART(yy,SRC.date_of_match1)) as int),

			TGT.Full_Date    =	SRC.date_of_match1,

			TGT.Day_Of_Week_ =  DATENAME(dw,SRC.date_of_match1),

			TGT.Day_Type     =  CHOOSE(DATEPART(dw,SRC.date_of_match1),
								'Weekend','Weekday', 'Weekday', 'Weekday','Weekday', 'Weekday','Weekend'),

			TGT.Day_Of_Month_ = DATEPART(d,SRC.date_of_match1),

			TGT.Month_        = CHOOSE(DATEPART(m,SRC.date_of_match1),
								'January','February', 'March', 'April','May', 'June',
								'July','August','September','October','November','December'),

			TGT.Quarter_      = CONCAT('Q', DATEPART(q,SRC.date_of_match1)),

			TGT.Year_         = DATEPART(yy,SRC.date_of_match1)

WHEN NOT MATCHED BY TARGET
THEN INSERT (
			 Date_ID,
             Full_Date,
			 Day_Of_Week_,
			 Day_Type,
			 Day_Of_Month_,
			 Month_,
			 Quarter_,
			 Year_
			 )
     VALUES (
			CAST(CONCAT( DATEPART(d,SRC.date_of_match1),
		   	DATEPART(m,SRC.date_of_match1),DATEPART(yy,SRC.date_of_match1)) as int),
			SRC.date_of_match1,
			DATENAME(dw,SRC.date_of_match1),
			CHOOSE(DATEPART(dw,SRC.date_of_match1),
			'Weekend','Weekday', 'Weekday', 'Weekday','Weekday', 'Weekday','Weekend'),
		    DATEPART(d,SRC.date_of_match1),
			CHOOSE(DATEPART(m,SRC.date_of_match1),
			'January','February', 'March', 'April','May', 'June',
			'July','August','September','October','November','December'),
			CONCAT('Q', DATEPART(q,SRC.date_of_match1)),
			DATEPART(yy,SRC.date_of_match1)
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.date_of_match1 into Log_ETL_Date_Dim;

SELECT MergeAction, count(*)
FROM   Log_ETL_Date_Dim
GROUP BY MergeAction;


--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- ETL Query for ipl.dbo.Match_F

create table Log_ETL_Match_F
(
MergeAction Varchar(20),
match_id Int
);

MERGE ipl.dbo.Match_F TGT
USING o_ipl.dbo.matches SRC 
ON (1=1)
WHEN MATCHED
THEN UPDATE
     SET    TGT.Match_ID = SRC.id , 
			TGT.Date_ID = CAST(CONCAT( DATEPART(d,SRC.date_of_match),
			 DATEPART(m,SRC.date_of_match),DATEPART(yy,SRC.date_of_match)) as int),
            TGT.Season = SRC.season,
            TGT.DL_Applied= SRC.dl_applied,
			TGT.Win_By_Runs = SRC.win_by_runs,
			TGT.Win_By_Wickets =  SRC.win_by_wickets
			
WHEN NOT MATCHED BY TARGET
THEN INSERT (Match_ID,
             Date_ID, 
			 Season, 
			 DL_Applied, 
			 Win_By_Runs,
			 Win_By_Wickets)
     VALUES ( SRC.id,
	         CAST(CONCAT( DATEPART(d,SRC.date_of_match),
			 DATEPART(m,SRC.date_of_match),DATEPART(yy,SRC.date_of_match)) as int),
			 SRC.season, 
			 SRC.dl_applied, 
			 SRC.win_by_runs,
			 SRC.win_by_wickets
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.id into Log_ETL_Match_F;

SELECT MergeAction, count(*)
FROM   Log_ETL_Match_F
GROUP BY MergeAction;



------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- ETL Query for ipl.dbo.Teams_Info_Dim


create table Log_ETL_Teams_Info_Dim
(
MergeAction Varchar(20),
match_id Int,
inning Int
);

MERGE ipl.dbo.Teams_Info_Dim TGT
USING (
select distinct  match_ID, inning , batting_team, bowling_team from o_ipl.dbo.deliveries 
) SRC 
ON (1=1)
WHEN MATCHED
THEN UPDATE
     SET    TGT.MI_ID = CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10))), 
			TGT.Match_ID = SRC.Match_ID,
            TGT.Batting_Team = SRC.batting_team,
            TGT.Bowling_Team = SRC.bowling_team

WHEN NOT MATCHED BY TARGET
THEN INSERT (MI_ID,
             Match_ID, 
			 Batting_Team, 
			 Bowling_Team)
     VALUES ( CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10))),
	         SRC.Match_ID,
			 SRC.batting_team, 
			 SRC.bowling_team
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.match_id, SRC.inning into Log_ETL_Teams_Info_Dim;

SELECT MergeAction, count(*)
FROM   Log_ETL_Teams_Info_Dim
GROUP BY MergeAction;


--------------------------------------------------------------------------------------------------------------------------------


create table Log_ETL_Delivery_F
(
MergeAction Varchar(20),
match_id Int,
inning Int,
over_no Int,
ball Int
);


MERGE ipl.dbo.Delivery_F TGT
USING o_ipl.dbo.deliveries SRC
ON (SRC.match_id = TGT.Match_ID AND
	SRC.inning = TGT.Innings_ID AND
	SRC.over_no = TGT.Over_No AND
	SRC.ball = TGT.Ball)
WHEN MATCHED
THEN UPDATE
     SET    TGT.MIBO_ID = CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10)),'-',
							   cast(SRC.Over_No as varchar (10)),'-',
							   cast(SRC.Ball as varchar (10))), 
			TGT.MI_ID = CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10))),
			TGT.Match_ID = SRC.match_id,
            TGT.Innings_ID = SRC.inning,
            TGT.Over_No = SRC.over_no,
			TGT.Ball = SRC.ball,
			TGT.Is_Super_Over = SRC.is_super_over,
			TGT.Wide_Runs = SRC.wide_runs,
			TGT.Bye_Runs = SRC.bye_runs,
			TGT.Leg_Bye_Runs = SRC.legbye_runs,
			TGT.No_Ball_Runs = SRC.noball_runs,
			TGT.Penality_Runs = SRC.penalty_runs,
			TGT.Batsman_Runs = SRC.batsman_runs,
			TGT.Extra_Runs = SRC.extra_runs,
			TGT.Total_Runs = SRC.total_runs

WHEN NOT MATCHED BY TARGET
THEN INSERT (MIBO_ID,
			 MI_ID,
             Match_ID, 
			 Innings_ID, 
			 Over_No, 
			 Ball,
			 Is_Super_Over,
			 Wide_Runs,
			 Bye_Runs,
			 Leg_Bye_Runs,
			 No_Ball_Runs,
			 Penality_Runs,
			 Batsman_Runs,
			 Extra_Runs,
			 Total_Runs)
     VALUES ( CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10)),'-',
							   cast(SRC.Over_No as varchar (10)),'-',
							   cast(SRC.Ball as varchar (10))),
			 CONCAT (cast(SRC.Match_ID as varchar (10)),'-',
							   cast(SRC.inning as varchar (10))),
	         SRC.match_id,
			 SRC.inning, 
			 SRC.over_no, 
			 SRC.ball,
			 SRC.is_super_over, 
			 SRC.wide_runs, 
			 SRC.bye_runs,
			 SRC.legbye_runs,
			 SRC.noball_runs,
			 SRC.penalty_runs,
			 SRC.batsman_runs,
			 SRC.extra_runs,
			 SRC.total_runs
			)

WHEN NOT MATCHED BY SOURCE
THEN DELETE
OUTPUT  $action, SRC.match_id, SRC.inning, SRC.over_no, SRC.ball into Log_ETL_Delivery_F;



SELECT MergeAction, count(*)
FROM   Log_ETL_Delivery_F
GROUP BY MergeAction;







