/**
 * Created by spadolski on 7/26/16.
 */
/*
 * jQuery Shorten plugin 1.0.0
 *
 * Copyright (c) 2013 Viral Patel
 * http://viralpatel.net
 *
 * Dual licensed under the MIT license:
 *   http://www.opensource.org/licenses/mit-license.php
 */

 (function($) {
	$.fn.shorten = function (settings) {

		var config = {
			showChars: 5000,
			ellipsesText: "...",
			moreText: "more",
			lessText: "less"
		};

		if (settings) {
			$.extend(config, settings);
		}

		$(document).off("click", '.morelink');

		$(document).on({click: function () {

				var $this = $(this);
                    if ($this.hasClass('less')) {
                        $this.removeClass('less');
                        $this.html(config.moreText);
                    } else {
                        $this.addClass('less');
                        $this.html(config.lessText);
                    }
				$this.parent().prev().toggle();
				$this.prev().toggle();
				return false;
			}
		}, '.morelink');

		return this.each(function () {
			var $this = $(this);
			if($this.hasClass("item")) return;

			$this.addClass("shortened");
			var content = $this.html();
			if (content.length > config.showChars) {

                var currShowChar = content.indexOf("</a>", config.showChars)+5;
                var c = content.substr(0, currShowChar);
                var h = content.substr(currShowChar-1, content.length - currShowChar);

				var html = c + '<span class="moreellipses">' + config.ellipsesText + ' </span><span class="morecontent"><span>' + h + '</span> <a href="#" class="morelink">' + config.moreText + '</a></span>';
				$this.html(html);

			    $(".morecontent span[class!='item']").hide();

			}
		});

	};

 })(jQuery);