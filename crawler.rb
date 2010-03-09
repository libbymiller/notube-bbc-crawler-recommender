   require 'rubygems'
   require 'json/pure'
   require 'uri'
   require 'time'
   require 'open-uri'
   require 'net/http'
   require 'java'
   require 'date'
   require 'pp'

# Jena stuff - assumes TDB in top dir
    
   Dir.glob("TDB-0.8.4/lib/*.jar") { |jar| require jar }
     
   java_import "com.hp.hpl.jena.tdb.TDB"
   java_import "com.hp.hpl.jena.tdb.TDBFactory"
   java_import "com.hp.hpl.jena.util.FileManager"
   java_import "com.hp.hpl.jena.query.QueryExecutionFactory"
   java_import "com.hp.hpl.jena.query.ResultSetFormatter"

# Use union default graph

   TDB.getContext.set(TDB.symUnionDefaultGraph, true)
   dataset = TDBFactory.create_dataset("TDBstore")

# Makes a list of BBC scheduls URLs to retrieve for a given day (d1)

   def get_urls_to_retrieve(d1)
              urls = []

              if (d1==nil || d1 =="")
                 t = DateTime.now
                 d = t.strftime("%Y/%m/%d")
              else
                 d = d1
              end
              pt1 = "http://www.bbc.co.uk/"
              pt2 = "/programmes/schedules/"

              channel = "bbcone"
              url = "#{pt1}#{channel}#{pt2}london/#{d}.json"
              urls.push(url)

              channel = "bbctwo"
              url = "#{pt1}#{channel}#{pt2}england/#{d}.json"
              urls.push(url)

              channel = "bbcthree"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "bbcfour"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "bbchd"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "cbeebies"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "cbbc"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "parliament"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "bbcnews"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "radio4"
              url = "#{pt1}#{channel}#{pt2}fm/#{d}.json"

              channel = "radio1"
              url = "#{pt1}#{channel}#{pt2}england/#{d}.json"

              channel =  "1extra"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "radio2"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "radio3"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel = "5live" 
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "5livesportsextra"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "6music"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "asiannetwork"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              channel =  "worldservice"
              url = "#{pt1}#{channel}#{pt2}#{d}.json"
              urls.push(url)

              return urls
   end

# parses one of the BBC schedule URLs and gets a list of PIDs

   def get_single_url(url)

              useragent = "NotubeMiniCrawler/0.1"
              u =  URI.parse url  
              req = Net::HTTP::Get.new(u.request_uri,{'User-Agent' => useragent})
              begin    
                res2 = Net::HTTP.new(u.host, u.port).start {|http|http.request(req) }
              end

              r = ""
              begin
                 r = res2.body
              rescue OpenURI::HTTPError=>e
                 case e.to_s        
                    when /^404/
                       raise 'Not Found'
                    when /^304/
                       raise 'No Info'
                    end
              end
              return r
   end

# Crawls them, carefully and slowly, taking care to identify itself

   def get_urls(url, d)
              useragent = "NotubeMiniCrawler/0.1"
              u =  URI.parse url  
              req = Net::HTTP::Get.new(u.request_uri,{'User-Agent' => useragent})
              begin    
                res2 = Net::HTTP.new(u.host, u.port).start {|http|http.request(req) }
              end
              j = nil
              begin
                 j = JSON.parse(res2.body)
              rescue JSON::ParserError=>e
                 puts "Error "+e.to_s
                 return Array.new
              rescue OpenURI::HTTPError=>e
                 case e.to_s        
                    when /^404/
                       raise 'Not Found'
                    when /^304/
                       raise 'No Info'
                    end
                 return Array.new
              end
              txt = ""
              service=j["schedule"]["service"]["key"]
              serviceTitle=j["schedule"]["service"]["key"]
              arr = j["schedule"]["day"]["broadcasts"]
              arr2 = Array.new
              pids = Array.new
              arr.each do |x|
                  pid = x["programme"]["pid"]
                  tt1 = x["programme"]["display_titles"]["title"]
                  tt2 = x["programme"]["display_titles"]["subtitle"]
                  pidTitle = "#{tt1}: #{tt2}"
                  # fix up &amps;
                  pidTitle.gsub!("&","&amp;")
                  startd = x["start"]
                  endd = x["end"]
                  #puts "start #{startd} date to match #{d} ''"
                  if (startd.match(d))
                    pids.push(pid)
                    arr2.push({"pid"=>pid,"displayTitle"=>pidTitle,"startd"=>startd,"endd"=>endd,"service"=>service,"serviceTitle"=>serviceTitle})
                  end
              end
              return arr2,pids
   end

# Utility to save files

   def save(dir, data, filename)
            FileUtils.mkdir_p dir
            fn = dir+"/"+filename 
            puts fn
            open(fn, 'w') { |f|
              f.puts data
              f.close
            }
   end


# Main method: gets all the BBC schedule URLs, turns the schedule data into RDF, gets all the RDF files for everything on 
# today, and puts them all in a directory, also adding them to Jena TDB at the same time.

   begin

# we assume today if no arg given

     t = DateTime.now
     dx = t.strftime("%Y/%m/%d")
     dy = t.strftime("%Y-%m-%d")
     if (ARGV[0])
       dx = ARGV[0]
       dy = ARGV[0].gsub("/","-")
       puts "Getting data for date: #{dy} #{dx}"
     else
       puts "Assuming today; use jruby crawler.rb 2010/03/01 for a specific day"
     end

     base = "http://www.bbc.co.uk/"

     urls =  get_urls_to_retrieve(dx)
     #puts urls

# useful smaller array if you aren't sure if it's working
     #urls =["http://www.bbc.co.uk/bbcone/programmes/schedules/london/2010/03/10.json","http://www.bbc.co.uk/bbctwo/programmes/schedules/england/2010/03/10.json"]     

     arrs = Array.new
     allprogs = Array.new

     #puts "sending date #{dy}"
     urls.each do |u|
        arr,pids = get_urls(u,dy)
        puts "sleeping 5 #{u} ... #{arr.class}"
        if pids 
           puts "Pids found: #{pids.length}"
        end
        sleep 5
        arrs.push(pids)
        allprogs.push(arr)
     end

     allpids = arrs.flatten
     #puts "allpids #{allpids}"

# might need this at some point.
     save("crawler/#{dy.to_s}", allpids,"pids.txt")     


# generate the rdf for the scehdule

     rdf = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<rdf:RDF xmlns:rdf      = \"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"
         xmlns:rdfs     = \"http://www.w3.org/2000/01/rdf-schema#\"
         xmlns:po       = \"http://purl.org/ontology/po/\"
         xmlns:time     = \"http://www.w3.org/2006/time#\"
         xmlns:dc       = \"http://purl.org/dc/elements/1.1/\"
         xmlns:timeline = \"http://purl.org/NET/c4dm/timeline.owl#\"
         xmlns:event    = \"http://purl.org/NET/c4dm/event.owl#\">
"

     allprogs.each do |progs|
         if progs!=nil
           progs.each do |prog|
             pid = prog["pid"]
             displayTitle = prog["displayTitle"]
             startd = prog["startd"]
             startdd = startd.gsub(/T\d\d:\d\d:\d\dZ/,"")
             endd = prog["endd"]
             service = prog["service"]
             serviceTitle = prog["serviceTitle"] #not used
             rdfpart = "
<po:FirstBroadcast>
  <po:schedule_date rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">#{startdd}</po:schedule_date>
  <event:time>
    <timeline:Interval>
      <timeline:start rdf:datatype=\"http://www.w3.org/2001/XMLSchema#dateTime\">#{startd}</timeline:start>
      <timeline:end rdf:datatype=\"http://www.w3.org/2001/XMLSchema#dateTime\">#{endd}</timeline:end>
    </timeline:Interval>
  </event:time>
  <po:broadcast_on rdf:resource=\"http://www.bbc.co.uk/programmes/#{service}#service\"/>
  <po:broadcast_of rdf:resource=\"http://www.bbc.co.uk/programmes/#{pid}#programme\"/>
  <dc:title>#{displayTitle}</dc:title>
</po:FirstBroadcast>
"    
             rdf = rdf + rdfpart
           end
        end
     end
     
     puts "sleeping 10 then getting the data"
     sleep 10

     
# now crawl the RDF urls

     allpids.each do |pid|
        if (pid && pid!="")
          begin
            puts "sleeping 5"
            sleep 5
            st = "http://www.bbc.co.uk/programmes/#{pid}.rdf"
            puts "getting #{st}"
            data = get_single_url(st)
            fullpath = "crawler/#{dy.to_s}/data/#{pid}.rdf"
            save("crawler/#{dy.to_s}/data", data,"#{pid}.rdf")             
            # inject into rdfdb
            nm = dataset.get_named_model(fullpath)
            nm.remove_all
            FileManager.get.read_model(nm, fullpath, base, "RDF/XML")
          rescue Exception=>e
            puts "Error #{e}"
          end
        end
     end

     # save the schedule to the same directory 

     rdf = rdf + "</rdf:RDF>\n"
     # inject into rdfdb
     filen = "schedule.rdf"
     fullpath = "crawler/#{dy.to_s}/data/#{filen}"
     save("crawler/#{dy.to_s}/data", rdf,filen)

     begin
       nm = dataset.get_named_model(fullpath)
       nm.remove_all
       FileManager.get.read_model(nm, fullpath, base, "RDF/XML")
     rescue Exception=>e
       puts "Error #{e}"
     end

   end

