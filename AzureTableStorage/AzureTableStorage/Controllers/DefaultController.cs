using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace AzureTableStorage.Controllers
{
    public class DefaultController : Controller
    {
        // GET: Default
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult WorkingWithTableStorage()
        {
            Models.MyTableStorage ObjectMyTableStorage = new Models.MyTableStorage();

            ObjectMyTableStorage.Create_a_table();
            ObjectMyTableStorage.Add_an_entity_to_a_table();
            ObjectMyTableStorage.Add_a_batch_of_entities();
            ObjectMyTableStorage.Retrieve_all_entities_in_a_partition();
            ObjectMyTableStorage.Retrieve_a_range_of_entities_in_a_partition();
            ObjectMyTableStorage.Retrieve_a_single_entity();
            ObjectMyTableStorage.Replace_an_entity();
            ObjectMyTableStorage.Insert_or_replace_an_entity();
            ObjectMyTableStorage.Query_a_subset_of_entity_properties();
            ObjectMyTableStorage.Delete_an_entity();
            ObjectMyTableStorage.Delete_a_table();

            return View("DefaultView");
        }
    }
}