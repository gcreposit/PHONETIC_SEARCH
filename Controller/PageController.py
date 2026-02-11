# Controllers/PageController.py

from flask import Blueprint, render_template

page_bp = Blueprint(
    "page_bp",
    __name__,
    template_folder="../Templates"  # ensure it points to templates folder
)


@page_bp.route("/tabular-view", methods=["GET"])
def tabluar_view():
    return render_template("tabularView.html")
