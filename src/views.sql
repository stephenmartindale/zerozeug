CREATE VIEW GameVictor AS
  SELECT id as game_id, match_id as match_id,
    case victor when 1 then black else white end as victor
    FROM Game;


CREATE VIEW MatchVictor AS
  SELECT id as match_id, start_date, challenger, defender,
    challenger_victories, defender_victories,
    case when (challenger_victories > defender_victories) then challenger else defender end as victor
    FROM (SELECT
      Match.*,
      SUM(case GameVictor.victor when challenger then 1 else 0 end) as challenger_victories,
      SUM(case GameVictor.victor when defender then 1 else 0 end) as defender_victories
      FROM Match
      JOIN GameVictor on GameVictor.match_id = Match.id
      GROUP BY Match.id);
