# Pre-Data Thoughts
## Lessons from Big Data Bowl
1. Get familiar with the data ASAP
    - plot or animate the data so you can visualize what is happening
    - calculate something easy to make sure your math turns out the way you expect
2. The final project should be a single, well developed idea
    - communicating complex ideas is key and trying to string multiple complex ideas together can be difficult and messy
## Possible Project Ideas
1. Score Card (Draft/Free Agent pre-season scouting)
    - contact quality
        * calculation for sweet spot location
        * distance between sweet spot and ball
    - "seeing the ball"
        * Note from podcast - Quality swings can come in different forms (grounders, foul balls, etc) not only home runs. But they all start with seeing the pitch and swinging in the right place.
    - bat angle through the zone
    - swing decision/chase rate
    - 
2. Batter Tendencies (Pitcher pre-game scouting)
3. Identifying the best matchups
4. Line up Building (who does best against the opposition pitcher/bull pen)
5. Post Game Review
6. In Game Adjustments
7. Predicting Swing Outcomes
    - not sure this one is possible, but it might be
    - if it is possible, it might not be useful
8. Pitcher Deception Score
    - why do some pitchers have more swing and miss than others? 
    - tunneling 
        * tunnel point is the point where a batter needs to commit to their swing
        * tunneling is pitching a secondary pitch that looks like a primary pitch until the tunnel point, but breaks differently
9. Do players adjust their swing when they already have 2 strikes?
    - shorten swing
    - level swing
    - attack angle
    - bat speed
## Ideas to explore the data
1. average pitch speed for strikeouts
2. average bat speed/acceleration at contact point
3. bat-ball distance at frame x
4. sweet spot
5. frame of contact
    - predict/determine which frame the ball crosses the plane of the bat
6. frame of the wrist break/roll
    - the batter has to rotate their wrists during the swing, and I remember thinking about when the wrist breaks when I was playing
    - I'm not sure this is possible or if it even happens in a single frame
7. swing heat map
    - swing heat map
    - contact heat map
    - speed of bat heat map
8. create a dataframe summarizing every swing in the dataset
    - xy location of ball when it crosses the plate
    - ball/strike/in play outcome
    - pitch type
9. contact angles
    - attack angle: angle of bat **path** at contact relative to the ground
    - vertical bat angle: angle of barrel at contact relative to the ground