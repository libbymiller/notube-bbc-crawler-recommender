require 'java'
require 'date'
require 'pp'

Dir.glob("TDB-0.8.4/lib/*.jar") { |jar| require jar }

java_import "com.hp.hpl.jena.tdb.TDB"
java_import "com.hp.hpl.jena.tdb.TDBFactory"
java_import "com.hp.hpl.jena.util.FileManager"
java_import "com.hp.hpl.jena.query.QueryExecutionFactory"
java_import "com.hp.hpl.jena.query.ResultSetFormatter"


class Auth < java.net.Authenticator
  @username = "foo"
  @password = "bar"

  def setUserPass(u,p)
     @username=u
     @password=p
  end

  def getPasswordAuthentication
    return make_auth(@username, @password)
  end
  
  def make_auth(user, password)
    pass = java.lang.String.new(password).to_char_array
    java.net.PasswordAuthentication.new(user, pass)
  end
end


def read_and_generate(d1,profile,ur,pa)

  d = "nodategiven"

  if (d1==nil || d1 =="")
    t = DateTime.now
    d = t.strftime("%Y-%m-%d")
  else
    d = d1
  end

  #puts "date #{d1} profile #{profile}"

# Use union default graph
  TDB.getContext.set(TDB.symUnionDefaultGraph, true)

  dataset = TDBFactory.create_dataset("TDBstore")
  base = "http://www.bbc.co.uk/"

# read in the profile
  u = profile.to_s

  if (ur && pa)
    a = Auth.new
    a.setUserPass(ur.to_s,pa.to_s)
    java.net.Authenticator.setDefault(a)
  end

  profileDataset = TDBFactory.create_dataset("TDBprofileStore")

# deal with file uris
  if (!u.match("^http://"))
    u = File.expand_path(u)
    u = "file:///#{u}"
  end

  nm1 = profileDataset.get_named_model(u)
  nm1.remove_all

# find out the wi:topic, weight etc

  FileManager.get.read_model(nm1, u)

  query = "
PREFIX wi: <http://xmlns.notu.be/wi#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
SELECT DISTINCT ?url ?label ?weight ?scale ?reason
where { graph <#{u}> {
  ?thing wi:topic ?url .
  ?thing rdfs:label ?label .
  ?thing wi:weight ?weight .
  ?thing wi:scale ?scale .
  ?thing wi:reason ?reason .
}}
ORDER BY ?name DESC(?weight)"

# assumes all the same context

  qe = QueryExecutionFactory.create(query, profileDataset)
  res = qe.exec_select
#  ResultSetFormatter.out(java.lang.System.out, res)

# for each of these query the other db and see if there's anything useful
# if there is, generate a reason

  finalResults = {}

  res.each do |r|
    url = r.get("url").to_s 
    label = r.get("label").to_s 
    weight = r.get("weight").to_s 
    scale = r.get("scale").to_s 
    reason = r.get("reason").to_s

    q = "SELECT ?k ?service ?start ?end ?title where {
{{?k <http://purl.org/ontology/po/genre> <#{url}>} UNION {?k <http://purl.org/ontology/po/episode> <#{url}>}} .
?z <http://purl.org/ontology/po/schedule_date> \"#{d}\"^^<http://www.w3.org/2001/XMLSchema#date> .
?z <http://purl.org/ontology/po/broadcast_of> ?k .
?z <http://purl.org/ontology/po/broadcast_on> ?service .
?z <http://purl.org/NET/c4dm/event.owl#time> ?i .
?i <http://purl.org/NET/c4dm/timeline.owl#start> ?start .
?i <http://purl.org/NET/c4dm/timeline.owl#end> ?end . 
?z <http://purl.org/dc/elements/1.1/title> ?title .
}"

    qe1 = QueryExecutionFactory.create(q, dataset)
    res1 = qe1.exec_select
#    ResultSetFormatter.out(java.lang.System.out, res1)

# de-dupe 
# also need to add 'reason' 

    res1.each do |rr|
      prog = rr.get("k").to_s
      channel=rr.get("service").to_s
      startd=rr.get("start").to_s
      endd=rr.get("end").to_s
      dts = "^^http://www.w3.org/2001/XMLSchema#dateTime"
      startd.gsub!(dts,"")
      endd.gsub!(dts,"")
      title=rr.get("title").to_s
      score="low"
      if (weight.to_f > 1)
        score = "high"
      end

      foo = []
      if (finalResults[prog]!=nil)
         foo = finalResults[prog]
      end
      foo.push({"programme"=>prog, "channel"=>channel, "start"=>startd, "end"=>endd,"title"=>title, "reason"=>[reason], "score"=>score})
      finalResults[prog]=foo
    end
  end
  pp finalResults
#  return finalResults
end

begin
  if (ARGV[0] && ARGV[1])
    if (ARGV.length<3)
      read_and_generate(ARGV[0],ARGV[1], nil, nil)
    else
      read_and_generate(ARGV[0],ARGV[1],ARGV[2],ARGV[3])
    end
  else
    puts "Usage: jruby recommend.rb YYYY-mm-dd profile_url_or_file [username] [password]"
  end
end

