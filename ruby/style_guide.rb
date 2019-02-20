def a_thing(
  author,
  title
  )

  phrase = "default"

  case
  when author == "Vonnegut"
    phrase = "Kurt!"
  when title == "The Eyre Affair"
    phrase = "Thursday Next"
  when title == "Grapes of Wrath"
    phrase = "Wrong!"
  else
    phrase = "1984"
  end

  response = case phrase
             when "1984" then "George Orwell"
             when "Kurt!" then "Stephen\'s favorite"
             when "Thursday Next" then "Like my kittycat"
             when "Wrong!" then "You know better"
             else "What are we even doing here?"
             end

  puts phrase, response

end
