using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class PagedList<T>
    {
        public List<T> Items { get; set; }
        public int PageIndex { get; set; }
        public int NumberOfPages { get; set; }
        public int NumberPerPage { get; set; }
    }

    public static class PagedListExtensions
    {
        public static PagedList<T> FromRequest<T>(this PagedList<T> pagedList, PagedRequest request, int count)
        {
            pagedList.NumberPerPage = request.NumberPerPage;
            pagedList.PageIndex = request.PageIndex;
            pagedList.NumberOfPages = (int)Math.Ceiling(count / (double)pagedList.NumberPerPage);
            if (pagedList.PageIndex >= pagedList.NumberOfPages)
            {
                pagedList.PageIndex = pagedList.NumberOfPages - 1;
            }
            if (pagedList.PageIndex < 0) pagedList.PageIndex = 0;
            return pagedList;
        }
    }
}
