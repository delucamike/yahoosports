require "mysql"
require "nokogiri"
require "mechanize"
Mysql::Result.send(:include, Enumerable)

#-----------------------------------------------------------------------------------------------
def is_number(number)
  true if Float(number) rescue false
end

#-----------------------------------------------------------------------------------------------
class DBSyntax

#-----------------------------------------------------------------------------------------------
def get_schema(db, table)

    sql = "EXPLAIN " + table
    rows = db.query(sql)
    rows_ = Array.new

    while row = rows.fetch_row
          rows_.push(row)
    end

    return rows_
end

#-----------------------------------------------------------------------------------------------
def escape_val(val)
    return val.gsub(/'/, "\\'")
end

#-----------------------------------------------------------------------------------------------
def insert_str(db, table, opts)
    db.query("SET FOREIGN_KEY_CHECKS=0;")

    str = String.new("")
    str += "INSERT INTO `" +  table + "` ("

        opts.each { |k,v|
          if not v then
            v = ""
          end
          if k then
            str += "`" + String.new(k.to_s) + "`,"
          end
        }

        str = str.gsub(/,$/, "")

    str += ") VALUES ("

        opts.each { |k, v|
          if not v then
            v = ""
          end
          if k and v then
            str += "'" + String.new(self.escape_val(v.to_s)) + "',"
          end
          if not v then
            str += "'',"
          end
        }

        str = str.gsub(/,$/, "")

    str += ")"
    #puts str
    return str
end

#-----------------------------------------------------------------------------------------------
def update_str(db, table, key, value, opts)
    self.foreign_key_checks_off(db)

    str = String.new("")

    str += "UPDATE `" + table + "` SET "
        opts.each { |k, v|
          if not v then
            v = ""
          end
          if k and v then
            str += "`" + String.new(k.to_s) + "`" + " = '" + String.new(v.to_s) + "', "
          end
        }
        str = str.gsub(/,\s$/,"")

    str += " WHERE " + "`" + String.new(key.to_s) + "` = '" + String.new(self.escape_val(value.to_s)) + "'"

    return str
end

#-----------------------------------------------------------------------------------------------
def update_str_with_conditionals(db, table, conditions = {}, data)
    self.foreign_key_checks_off(db)

    str = String.new("")

    str += "UPDATE `" + table + "` SET "

    data.each do |k, v|
         if not v then
            v = ""
         end
         if k and v then
            str += "`" + String.new(k.to_s) + "`" + " = '" + String.new(v.to_s) + "', "
         end
    end

    str = str.gsub(/,\s$/,"")

    unless conditions.empty?
        where_conditions = conditions.map { |key, value| "`#{key}` = '#{self.escape_val(value.to_s)}'" }
        where_conditions = where_conditions.join(' AND ')

        str += " WHERE #{where_conditions}"
    end

    return str
end

#-----------------------------------------------------------------------------------------------
end

#-----------------------------------------------------------------------------------------------
class SportsScraper

#-----------------------------------------------------------------------------------------------
def initialize(league, task_logger, scrape_date)
    puts "----------------------------------------------------------------------------"
    puts "********Initialize********"
    @datestr = '2015-04-29'
    @task_logger = task_logger
    @league = league.downcase
    @siteurl = 'http://sports.yahoo.com'
    @client = Mechanize.new()
    @gameURLs = Array.new()
    @playerStats = Array.new()

    rails_db_config = Rails.application.config.database_configuration[Rails.env]
    @host = rails_db_config['host']
    @username = rails_db_config['username']
    @pass = rails_db_config['password']
    @port = rails_db_config['port']
    @table = rails_db_config['database']
    @db = Mysql.connect(@host, @username, @pass, @table)
    @dbsyntax = DBSyntax.new()

    if @league == "mlb"
        @sport = 'Baseball'
        @leagueId = '19'
    elsif @league == "nba"
        @sport = 'Basketball'
        @leagueId = '10'
    end

    @playerSchema = @dbsyntax.get_schema(@db, "Players")
    @playerStatsSchema = @dbsyntax.get_schema(@db, "PlayerStats_#{@sport}")
    @teamSchema = @dbsyntax.get_schema(@db, "Teams")
    @teamStatsSchema = @dbsyntax.get_schema(@db, "TeamStats_#{@sport}")
    @gameSchema = @dbsyntax.get_schema(@db, "Games")

    puts "Current Date: #{@datestr}"
    puts "League: #{@league}"
end

#-----------------------------------------------------------------------------------------------
def start
    puts "********Start********"
    url = @siteurl + "/#{@league}/scoreboard/?date=#{@datestr}&conf="
    result = @client.get(url)
    parser = result.parser

    if @league == "mlb"
        links = parser.xpath("//div/table/tbody[contains(@class, 'game')]")
    else
        links = parser.xpath("//div/table/tbody/tr[contains(@class, 'game')]")
    end

    links.each { |link|
      _id = link.attr("data-url")
      @gameURLs.push(_id)
    }

    self.parse_games
end

#-----------------------------------------------------------------------------------------------
def parse_games
    puts "********Parse Games********"
    @gameURLs.each { |k|
        @playerStats.clear
        url = @siteurl + k
        puts "Game URL: #{url}"
        result = @client.get(url)
        parser = result.parser

        gameTime = parser.xpath("//li[@class='left']")
        gameTime = gameTime.children[0].inner_text
        puts "Game Time: #{gameTime}"
        gameTime = Time.zone.parse(gameTime).strftime("%Y-%m-%d %H:%M:%S")

        if parser.xpath("//div[contains(@class, 'status-final')]") then
            @inProgress = 0
            puts "Game Status: Final"
        elsif parser.xpath("//div[contains(@class, 'status-pregame')]") then
            @inProgress = 2
            puts "Game Status: Not Started"
        elsif parser.xpath("//div[contains(@class, 'status-delayed')]") then
            @inProgress = 2
            puts "Game Status: Delayed"
        elsif parser.xpath("//div[contains(@class, 'status-postponed')]") then
            @inProgress = 2
            puts "Game Status: Postponed"
        else
            @inProgress = 1
            puts "Game Status: In Progress"
        end


        away_image_url = parser.xpath("//div[contains(@class,'team away')]/a/span/img").attr('src')
        home_image_url = parser.xpath("//div[contains(@class,'team home')]/a/span/img").attr('src')
        puts "Image URL: #{away_image_url}"

        away_team_url = @siteurl + parser.xpath("//div[contains(@class,'team away')]/a").attr('href')
        home_team_url = @siteurl + parser.xpath("//div[contains(@class,'team home')]/a").attr('href')
        puts "Team URL: #{away_team_url}"

        away_team_abbr = away_team_url.split(//).last(4).join.chomp('/').upcase
        home_team_abbr = home_team_url.split(//).last(4).join.chomp('/').upcase
        puts "Team Abbr: #{away_team_abbr}"


        @insertAwayTeam = 0
        @insertHomeTeam = 0
        @gameExists = self.game_exists(url)
        @awayTeamExists = self.team_exists(away_team_url)
        @homeTeamExists = self.team_exists(home_team_url)

        if not @gameExists
            away_team_name = parse_away_team_page(away_team_url)
            if not @awayTeamExists
                @insertAwayTeam = 1
            end
        end

        if not @gameExists
            home_team_name = parse_home_team_page(home_team_url)
            if not @homeTeamExists
                @insertHomeTeam = 1
            end
        end

        teams = {
            "away" => {
                "TeamFullName" => away_team_name,
                "TeamName" => away_team_name,
                "TeamPrefix" => away_team_abbr,
                "ESPNUrl" => away_team_url,
                "LeagueID" => @leagueId,
                "ImageURL" => away_image_url
            },
            "home" => {
                "TeamFullName" => home_team_name,
                "TeamName" => home_team_name,
                "TeamPrefix" => home_team_abbr,
                "ESPNUrl" => home_team_url,
                "LeagueID" => @leagueId,
                "ImageURL" => home_image_url
            }
        }

        @insertGame = 0
        @updateGame = 0
        @gameExists = self.game_exists(url)


        if not @gameExists
            @insertGame = 1
        elsif @gameExists && @inProgress == 1
            @updateGame = 1
        end

        gameTitle = "#{away_team_name} vs. #{home_team_name}"
        puts "Game Title: #{gameTitle}"

        game = {
            "GameID" => 1,
            "LeagueID" => @leagueId,
            "GameTitle" => gameTitle,
            "HomeTeamID" => @homeTeamID,
            "AwayTeamID" => @awayTeamID,
            "StartDate" => gameTime,
            "InProgress" => @inProgress,
            "ESPNUrl" => url
        }

        if @insertAwayTeam == 1
            self.insert_team(teams['away'])
        end

        if @insertHomeTeam == 1
            self.insert_team(teams['home'])
        end

        if @insertGame == 1
            self.insert_game(game)
        end

        if @updateGame == 1
            self.update_game(game)
        end

        self.process_players(parser)
    }
end

#-----------------------------------------------------------------------------------------------
def parse_away_team_page(away_team_url)
    puts "********Parse Away Team Page********"
    away_team_page = @client.get(away_team_url)
    away_team_parser = away_team_page.parser

    away_team_name = away_team_parser.xpath("//div[@class='team-info']/h1").inner_text

    return away_team_name
end

#-----------------------------------------------------------------------------------------------
def parse_home_team_page(home_team_url)
    puts "********Parse Home Team Page********"
    home_team_page = @client.get(home_team_url)
    home_team_parser = home_team_page.parser

    home_team_name = home_team_parser.xpath("//div[@class='team-info']/h1").inner_text

    return home_team_name
end

#-----------------------------------------------------------------------------------------------
def team_exists(teamURL)
    puts "********Team Exists********"
    query = @db.query("SELECT TeamID FROM `Teams` WHERE ESPNUrl = '" + teamURL + "' LIMIT 1;")
    team = {}

    if query.num_rows > 0 then
        vals = 0

        while row = query.fetch_row
            cnt = 0
            row.each do |row_value|
                if cnt == 0
                team[@gameSchema[vals]] =  row_value
                vals += 1
                end
            end
            cnt = 0
        end
        return team
    end

    return false
end

#-----------------------------------------------------------------------------------------------
def game_exists(gameURL)
    query = @db.query("SELECT GameID FROM `Games` WHERE ESPNUrl = '" + gameURL + "' LIMIT 1;")
    game = {}

    if query.num_rows > 0 then
        vals = 0

        while row = query.fetch_row
            cnt = 0
            row.each do |row_value|
                if cnt == 0
                game[@gameSchema[vals]] =  row_value
                vals += 1
                end
            end
            cnt = 0
        end
        return game
    end

    return false
end

#-----------------------------------------------------------------------------------------------
def insert_team(team)
    puts "********Insert Team********"
    time = Time.new
    createdDate = time.strftime("%Y-%m-%d %H:%M:%S")
    modifiedDate = createdDate
    q = @dbsyntax.insert_str(@db, "Teams", {
       "TeamId" => team['TeamId'],
       "TeamPrefix" => team['TeamPrefix'],
       "TeamName" => team['TeamName'],
       "TeamFullName" => team['TeamFullName'],
       "ESPNUrl" => team['ESPNUrl'],
       "LeagueID" => team['LeagueID'],
       "createdDate" => createdDate,
       "modifiedDate" => modifiedDate
    })

    @task_logger.increment(:records_inserted)
    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def insert_game(game)
    puts "********Insert Game********"
    time = Time.new
    createdDate = time.strftime("%Y-%m-%d %H:%M:%S")
    modifiedDate = createdDate
    q = @dbsyntax.insert_str(@db, "Games", {
       "LeagueID" => @leagueId,
       "GameTitle" => game['GameTitle'],
       "HomeTeamID" => game['HomeTeamID'],
       "AwayTeamID" => game['AwayTeamID'],
       "Attendance" => game['Attendance'],
       "StartDate" => game['StartDate'],
       "InProgress" => game['InProgress'],
       "ESPNUrl" => game['ESPNUrl'],
       "createdDate" => createdDate,
       "modifiedDate" => modifiedDate
    })

    @task_logger.increment(:records_inserted)
    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def update_game(game)
    puts "********Update Game********"
    time = Time.new
    modifiedDate = time.strftime("%Y-%m-%d %H:%M:%S")
    q = @dbsyntax.update_str(@db, "Games", "GameID", @gameId, {
       "LeagueID" => @leagueId,
       "GameTitle" => game['GameTitle'],
       "HomeTeamID" => game['HomeTeamID'],
       "AwayTeamID" => game['AwayTeamID'],
       "Attendance" => game['Attendance'],
       "StartDate" => game['StartDate'],
       "InProgress" => game['InProgress'],
       "ESPNUrl" => game['ESPNUrl'],
       "modifiedDate" => modifiedDate
    })

    @task_logger.increment(:records_updated)
    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def process_players(parser)
    puts "********Process Players********"
    players = parser.xpath("//div[@class='data-container']/table/tbody")
    players.each { |k|
        @playerStats.push(k)
    }

end

end