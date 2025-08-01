const fs = require('fs');

// Load the JSON file
const data = JSON.parse(fs.readFileSync('app.json', 'utf8'));

// Target ID to search
const targetID = "8912";


// Search in 'takes' array
const results = data.takes.filter(entry => entry.ID === targetID ); // check both ID and s_ID just in case

if (results.length > 0) {
  console.log(`Found ${results.length} records for ID ${targetID}:`);
  console.log(results);

  const courseCount = {};
  results.forEach(entry => {
    const course = entry.course_id;
    courseCount[course] = (courseCount[course] || 0) + 1;
  });

  const duplicates = Object.entries(courseCount).filter(([_, count]) => count > 1);

  if (duplicates.length > 0) {
    console.log("\n Duplicate course_id(s) found for this ID:");
    duplicates.forEach(([courseId, count]) => {
      console.log(`- ${courseId}: ${count} times`);
    });
  } else {
    console.log("\nNo duplicate course_id found for this ID.");
  }

} else {
  console.log(`No records found for ID ${targetID}.`);
}
