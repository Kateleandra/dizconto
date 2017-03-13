var IPController = {

    index: function(req, res) {
        res.json({
            ip: req.ip
        });
    }
};

module.exports = IPController;
