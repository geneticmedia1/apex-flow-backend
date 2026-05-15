const express = require("express");

const app = express();

app.use(express.json());

app.post("/webhook", (req, res) => {

    const SECRET = "Tbot Automate HWR Pro";

    // CHECK SECRET
    if (req.body.secret !== SECRET) {

        console.log("INVALID SECRET");

        return res.status(403).send("Forbidden");
    }

    // RESPOND IMMEDIATELY
    res.sendStatus(200);

    // LOG AFTER RESPONSE
    console.log("VALID WEBHOOK RECEIVED:");
    console.log(req.body);
});

app.get("/", (req, res) => {
    res.send("Bot running");
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`Server running on ${PORT}`);
});