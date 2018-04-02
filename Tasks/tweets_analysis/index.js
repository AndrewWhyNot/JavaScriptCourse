'use strict';

const data = [];
let words = {};
let authors = {};
let post_countries = {};
let rt_countries = {};

let multiline_tweet = "";




function proceed_author(username, tw_data) {
    let {rts, folls} = tw_data;
    if (rts.length == 0)
        rts = "0";
    if (folls.length == 0)
        folls = "0";
    
    if (authors[username] == null)
        authors[username] = {"rts": parseInt(rts), "folls": parseInt(folls)};
    else {
        authors[username]["rts"] += parseInt(rts);
    }
}

function proceed_country(country, is_rt) {
    if (country.length == 0)
        country = "None";
    
    if (is_rt) {
        if (rt_countries[country] == null)
            rt_countries[country] = 1;
        else
            rt_countries[country] += 1;
    } else {
        if (post_countries[country] == null)
            post_countries[country] = 1;
        else
            post_countries[country] += 1;
    }
}

function proceed_text(text) {
    text = text.replace(/[.,\/#!?$%\^&\*;:{}=\-_`"~()]/g, "");
    text.split(" ").forEach(function(w) {
                    if (w.length != 0 && w != "RT" && w[0] != "@" && w.indexOf("http") == -1) {
                            if (words[w] == null)
                                words[w] = 1;
                            else
                                words[w] += 1;
                    }
    });
}
                          
function proceed_tweet(tw_data) {
    proceed_text(tw_data[6]);
    proceed_country(tw_data[11], tw_data[6].includes("RT"));
    proceed_author(tw_data[4], {"rts": tw_data[8], "folls": tw_data[14]})
}

const LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('./input/dataSet.csv');

lr.on('error', function (err) {
    // 'err' contains error object
});

//let counter = 0;
let headersLineRead = false;

lr.on('line', function (line) {

    if (!headersLineRead) {
        headersLineRead = true;
        return;
    }
    
    // pause emitting of lines...
    lr.pause();

    // ...do your asynchronous line processing..
    setTimeout(function () {
               // ...and continue emitting lines.
               if (line.split(";").length == 19)
                data.push(line);
               else {
                if (multiline_tweet.split(";").length == 19) {
                    data.push(multiline_tweet);
                    multiline_tweet = "";
                }
               }
               /*counter++;
               if (counter >= 500) {
                lr.end();
                return;
               }*/
               lr.resume();
      }, 100);
    
    let splitted = line.split(";");
    if (splitted.length != 19) {
      multiline_tweet += " " + line;
      splitted = multiline_tweet.split(";");
      if (splitted.length == 19) {
        proceed_tweet(splitted);
      }
    } else {
      proceed_tweet(splitted);
    }
      
});

lr.on('end', function () {
    // All lines are read, file is closed now.
    main();
});


// task 1. 10 Most frequent words
function getMostFreqWords() {
    return new Promise((resolve, reject) => {
                       let most_pop_words = [];
                       Object.keys(words).forEach(function(w) {
                                                  if (most_pop_words.length == 0) {
                                                    most_pop_words.push(w);
                                                    return;
                                                  }
                                                  let freq = words[w];
                                                  if (freq <= words[most_pop_words[most_pop_words.length - 1]]) {
                                                    if (most_pop_words.length < 10) {
                                                        most_pop_words.push(w);
                                                    }
                                                    return;
                                                  }
                                                  let ind = 0;
                                                  while (1) {
                                                    if (freq >= words[most_pop_words[ind]]) {
                                                        if (most_pop_words.length != 10)
                                                            most_pop_words.push(most_pop_words[most_pop_words.length-1]);
                                                        for (let i = most_pop_words.length-1; i > ind; i--) {
                                                            most_pop_words[i] = most_pop_words[i-1];
                                                        }
                                                        most_pop_words[ind] = w;
                                                        break;
                                                    }
                                                    ind++;
                                                  }
                                        });
                       resolve(most_pop_words);
                       });
}

// task 2. 10 Most popular tweets
function getMostPopularTweets() {
    return new Promise((resolve, reject) => {
                       let most_pop_tweets = [];
                       data.forEach(function(tw) {
                                    let splitted = tw.split(";");
                                    if (splitted.length != 19)
                                        return;
                                    let tw_data = {"text": splitted[6], "author": splitted[4], "rts": parseInt(splitted[8])};
                                    if (isNaN(tw_data.rts))
                                        tw_data.rts = 0;
                                    if (most_pop_tweets.length == 0) {
                                        most_pop_tweets.push(tw_data);
                                        return;
                                    }
                                    if (tw_data.rts <= most_pop_tweets[most_pop_tweets.length - 1].rts) {
                                        if (most_pop_tweets.length < 10) {
                                            most_pop_tweets.push(tw_data);
                                        }
                                        return;
                                    }
                                    let ind = 0;
                                    while (1) {
                                        if (tw_data.rts >= most_pop_tweets[ind].rts) {
                                            if (most_pop_tweets.length != 10)
                                                most_pop_tweets.push(most_pop_tweets[most_pop_tweets.length-1]);
                                            for (let i = most_pop_tweets.length-1; i > ind; i--) {
                                                most_pop_tweets[i] = most_pop_tweets[i-1];
                                            }
                                            most_pop_tweets[ind] = tw_data;
                                            break;
                                        }
                                        ind++;
                                    }
                            });
                       resolve(most_pop_tweets);
                       });
}

// task 3. 10 Most popular authors
function getMostPopularAuthors() {
    return new Promise((resolve, reject) => {
                       let most_pop_authors = [];
                       
                       function getAuthorLevel(a) {
                            let d = authors[a];
                            return d.rts + d.folls * 2;
                       }
                       
                       Object.keys(authors).forEach(function(au) {
                                                    if (most_pop_authors.length == 0) {
                                                        most_pop_authors.push(au);
                                                        return;
                                                    }
                                                    let lev = getAuthorLevel(au)
                                                    if (lev <= getAuthorLevel(most_pop_authors[most_pop_authors.length - 1])) {
                                                        if (most_pop_authors.length < 10) {
                                                            most_pop_authors.push(au);
                                                        }
                                                        return;
                                                    }
                                                    let ind = 0;
                                                    while (1) {
                                                        if (lev >= getAuthorLevel(most_pop_authors[ind])) {
                                                            if (most_pop_authors.length != 10)
                                                            most_pop_authors.push(most_pop_authors[most_pop_authors.length-1]);
                                                            for (let i = most_pop_authors.length-1; i > ind; i--) {
                                                                most_pop_authors[i] = most_pop_authors[i-1];
                                                            }
                                                            most_pop_authors[ind] = au;
                                                            break;
                                                        }
                                                        ind++;
                                                    }
                                        });
                       resolve(most_pop_authors);
                       });
}

// RESULTS
function outputResults(results) {

    console.log("1. 10 most frequently used words:");
    for (let i = 0; i < results[0].length; i++) {
        console.log(i+1, results[0][i], words[results[0][i]]);
    }
    
    console.log("\n\n2. 10 most popular tweets:");
    for (let i = 0; i < results[1].length; i++) {
        console.log(i+1, "@" + results[1][i].author);
        console.log(results[1][i].text);
        console.log(results[1][i].rts, "RTs");
    }
    
    console.log("\n\n3. 10 most popular authors:");
    for (let i = 0; i < results[2].length; i++) {
        let d = authors[results[2][i]];
        console.log(i+1, "@" + results[2][i], ": ", d.folls, "followers, ", d.rts, "RTs");
    }
    
    console.log("\n\n4.1. Countries creating content:");
    Object.keys(post_countries).forEach(function(c) {
                                        console.log(c, post_countries[c], "tweets");
                                        });
    console.log("\n4.2. Countries creating content:");
    Object.keys(rt_countries).forEach(function(c) {
                                      console.log(c, rt_countries[c], "RTs");
                                      });
}

function main () {
    
    let promises = [];
    promises.push(getMostFreqWords());
    promises.push(getMostPopularTweets());
    promises.push(getMostPopularAuthors());
    Promise.all(promises)
        .then((result) => { outputResults(result); });
    
}
